// Copyright Vespa.ai. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.model.application.validation;

import com.yahoo.config.model.deploy.DeployState;
import org.w3c.dom.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.logging.Level;
import java.util.regex.Pattern;

/**
 * A validator for bundles.
 *
 * @author hmusum
 * @author bjorncs
 */
public class BundleValidator extends AbstractBundleValidator {

    @Override
    protected void validateManifest(JarContext reporter, JarFile jar, Manifest mf) {
        // Check for required OSGI headers
        Attributes attributes = mf.getMainAttributes();
        HashSet<String> mfAttributes = new HashSet<>();
        for (Map.Entry<Object,Object> entry : attributes.entrySet()) {
            mfAttributes.add(entry.getKey().toString());
        }
        List<String> requiredOSGIHeaders = List.of(
                "Bundle-ManifestVersion", "Bundle-Name", "Bundle-SymbolicName", "Bundle-Version");
        for (String header : requiredOSGIHeaders) {
            if (!mfAttributes.contains(header)) {
                reporter.illegal("Required OSGI header '" + header + "' was not found in manifest in '" + filename(jar) + "'");
            }
        }

        if (attributes.getValue("Bundle-Version").endsWith(".SNAPSHOT")) {
            log(reporter.deployState(), Level.WARNING,
                    "Deploying snapshot bundle " + filename(jar) + ".\nTo use this bundle, you must include the " +
                            "qualifier 'SNAPSHOT' in the version specification in services.xml.");
        }

        if (attributes.getValue("Import-Package") != null) {
            validateImportedPackages(reporter.deployState(), jar, mf);
        }
    }

    @Override protected void validatePomXml(JarContext reporter, JarFile jar, Document pom) { }

    private void validateImportedPackages(DeployState state, JarFile jar, Manifest manifest) {
        Map<DeprecatedProvidedBundle, List<String>> deprecatedPackagesInUse = new HashMap<>();
        forEachImportPackage(manifest, (packageName) -> {
            for (DeprecatedProvidedBundle deprecatedBundle : DeprecatedProvidedBundle.values()) {
                for (Predicate<String> matcher : deprecatedBundle.javaPackageMatchers) {
                    if (matcher.test(packageName)) {
                        deprecatedPackagesInUse.computeIfAbsent(deprecatedBundle, __ -> new ArrayList<>())
                                .add(packageName);
                    }
                }
            }
        });
        deprecatedPackagesInUse.forEach((artifact, packagesInUse) -> {
            log(state, Level.WARNING, "JAR file '%s' imports the packages %s from '%s'. \n%s",
                filename(jar), packagesInUse, artifact.name, artifact.description);
        });
    }

    private enum DeprecatedProvidedBundle {
        ORG_JSON("org.json:json",
                "This bundle is no longer provided on Vespa 8 - " +
                        "see https://docs.vespa.ai/en/vespa8-release-notes.html#container-runtime.",
                Set.of("org\\.json")),
        JETTY("jetty", "The Jetty bundles are no longer provided on Vespa 8 - " +
                      "see https://docs.vespa.ai/en/vespa8-release-notes.html#container-runtime.",
              Set.of("org\\.eclipse\\.jetty.*"));

        final String name;
        final Collection<Predicate<String>> javaPackageMatchers;
        final String description;

        DeprecatedProvidedBundle(String name,
                                 String description,
                                 Collection<String> javaPackagePatterns) {
            this.name = name;
            this.javaPackageMatchers = javaPackagePatterns.stream()
                .map(s -> Pattern.compile(s).asMatchPredicate())
                .toList();
            this.description = description;
        }
    }
}
