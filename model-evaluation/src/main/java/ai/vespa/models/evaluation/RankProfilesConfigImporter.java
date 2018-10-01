// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package ai.vespa.models.evaluation;

import com.yahoo.collections.Pair;
import com.yahoo.config.FileReference;
import com.yahoo.filedistribution.fileacquirer.FileAcquirer;
import com.yahoo.io.GrowableByteBuffer;
import com.yahoo.io.IOUtils;
import com.yahoo.searchlib.rankingexpression.ExpressionFunction;
import com.yahoo.searchlib.rankingexpression.RankingExpression;
import com.yahoo.searchlib.rankingexpression.parser.ParseException;
import com.yahoo.tensor.Tensor;
import com.yahoo.tensor.TensorType;
import com.yahoo.tensor.serialization.TypedBinaryFormat;
import com.yahoo.vespa.config.search.RankProfilesConfig;
import com.yahoo.vespa.config.search.core.RankingConstantsConfig;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts RankProfilesConfig instances to RankingExpressions for evaluation.
 * This class can be used by a single thread only.
 *
 * @author bratseth
 */
public class RankProfilesConfigImporter {

    private final FileAcquirer fileAcquirer;

    public RankProfilesConfigImporter(FileAcquirer fileAcquirer) {
        this.fileAcquirer = fileAcquirer;
    }

    /**
     * Returns a map of the models contained in this config, indexed on name.
     * The map is modifiable and owned by the caller.
     */
    public Map<String, Model> importFrom(RankProfilesConfig config, RankingConstantsConfig constantsConfig) {
        try {
            Map<String, Model> models = new HashMap<>();
            for (RankProfilesConfig.Rankprofile profile : config.rankprofile()) {
                Model model = importProfile(profile, constantsConfig);
                models.put(model.name(), model);
            }
            return models;
        }
        catch (ParseException e) {
            throw new IllegalArgumentException("Could not read rank profiles config - version mismatch?", e);
        }
    }

    private Model importProfile(RankProfilesConfig.Rankprofile profile, RankingConstantsConfig constantsConfig)
            throws ParseException {

        List<Constant> constants = readLargeConstants(constantsConfig);

        Map<FunctionReference, ExpressionFunction> functions = new LinkedHashMap<>();
        Map<FunctionReference, ExpressionFunction> referencedFunctions = new LinkedHashMap<>();
        SmallConstantsInfo smallConstantsInfo = new SmallConstantsInfo();
        ExpressionFunction firstPhase = null;
        ExpressionFunction secondPhase = null;
        for (RankProfilesConfig.Rankprofile.Fef.Property property : profile.fef().property()) {
            Optional<FunctionReference> reference = FunctionReference.fromSerial(property.name());
            Optional<Pair<FunctionReference, String>> argumentType = FunctionReference.fromTypeArgumentSerial(property.name());
            Optional<FunctionReference> returnType = FunctionReference.fromReturnTypeSerial(property.name());
            if ( reference.isPresent()) {
                RankingExpression expression = new RankingExpression(reference.get().functionName(), property.value());
                ExpressionFunction function = new ExpressionFunction(reference.get().functionName(),
                                                                     Collections.emptyList(),
                                                                     expression);

                if (reference.get().isFree()) // make available in model under configured name
                    functions.put(reference.get(), function);
                // Make all functions, bound or not, available under the name they are referenced by in expressions
                referencedFunctions.put(reference.get(), function);
            }
            else if (argumentType.isPresent()) { // Arguments always follows the function in properties
                FunctionReference argReference = argumentType.get().getFirst();
                ExpressionFunction function = referencedFunctions.get(argReference);
                function = function.withArgument(argumentType.get().getSecond(), TensorType.fromSpec(property.value()));
                if (argReference.isFree())
                    functions.put(argReference, function);
                referencedFunctions.put(argReference, function);
            }
            else if (returnType.isPresent()) { // Return type always follows the function in properties
                ExpressionFunction function = referencedFunctions.get(returnType.get());
                function = function.withReturnType(TensorType.fromSpec(property.value()));
                if (returnType.get().isFree())
                    functions.put(returnType.get(), function);
                referencedFunctions.put(returnType.get(), function);
            }
            else if (property.name().equals("vespa.rank.firstphase")) { // Include in addition to functions
                firstPhase = new ExpressionFunction("firstphase", new ArrayList<>(),
                                                    new RankingExpression("first-phase", property.value()));
            }
            else if (property.name().equals("vespa.rank.secondphase")) { // Include in addition to functions
                secondPhase = new ExpressionFunction("secondphase", new ArrayList<>(),
                                                     new RankingExpression("second-phase", property.value()));
            }
            else {
                smallConstantsInfo.addIfSmallConstantInfo(property.name(), property.value());
            }
        }
        if (functionByName("firstphase", functions.values()) == null && firstPhase != null) // may be already included, depending on body
            functions.put(FunctionReference.fromName("firstphase"), firstPhase);
        if (functionByName("secondphase", functions.values()) == null && secondPhase != null) // may be already included, depending on body
            functions.put(FunctionReference.fromName("secondphase"), secondPhase);

        constants.addAll(smallConstantsInfo.asConstants());

        try {
            return new Model(profile.name(), functions, referencedFunctions, constants);
        }
        catch (RuntimeException e) {
            throw new IllegalArgumentException("Could not load model '" + profile.name() + "'", e);
        }
    }

    private ExpressionFunction functionByName(String name, Collection<ExpressionFunction> functions) {
        for (ExpressionFunction function : functions)
            if (function.getName().equals(name))
                return function;
        return null;
    }

    private List<Constant> readLargeConstants(RankingConstantsConfig constantsConfig) {
        List<Constant> constants = new ArrayList<>();

        for (RankingConstantsConfig.Constant constantConfig : constantsConfig.constant()) {
            constants.add(new Constant(constantConfig.name(),
                                       readTensorFromFile(constantConfig.name(),
                                                          TensorType.fromSpec(constantConfig.type()),
                                                          constantConfig.fileref())));
        }
        return constants;
    }

    protected Tensor readTensorFromFile(String name, TensorType type, FileReference fileReference) {
        try {
            File file = fileAcquirer.waitFor(fileReference, 7, TimeUnit.DAYS);
            if (file.getName().endsWith(".tbf"))
                return TypedBinaryFormat.decode(Optional.of(type),
                                                GrowableByteBuffer.wrap(IOUtils.readFileBytes(file)));
            else
                throw new IllegalArgumentException("Constant files on other formats than .tbf are not supported, got " +
                                                   file + " for constant " + name);
            // TODO: Support json and json.lz4
        }
        catch (InterruptedException e) {
            throw new IllegalStateException("Gave up waiting for constant " + name);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Collected information about small constants */
    private static class SmallConstantsInfo {

        private static final Pattern valuePattern = Pattern.compile("constant\\(([a-zA-Z0-9_.]+)\\)\\.value");
        private static final Pattern  typePattern = Pattern.compile("constant\\(([a-zA-Z0-9_.]+)\\)\\.type");

        private Map<String, TensorType> types = new HashMap<>();
        private Map<String, String> values = new HashMap<>();

        void addIfSmallConstantInfo(String key, String value) {
            tryValue(key, value);
            tryType(key, value);
        }

        private void tryValue(String key, String value) {
            Matcher matcher = valuePattern.matcher(key);
            if (matcher.matches())
                values.put(matcher.group(1), value);
        }

        private void tryType(String key, String value) {
            Matcher matcher = typePattern.matcher(key);
            if (matcher.matches())
                types.put(matcher.group(1), TensorType.fromSpec(value));
        }

        List<Constant> asConstants() {
            List<Constant> constants = new ArrayList<>();
            for (Map.Entry<String, String> entry : values.entrySet()) {
                TensorType type = types.get(entry.getKey());
                if (type == null) throw new IllegalStateException("Missing type of '" + entry.getKey() + "'"); // Won't happen
                constants.add(new Constant(entry.getKey(), Tensor.from(type, entry.getValue())));
            }
            return constants;
        }

    }

}
