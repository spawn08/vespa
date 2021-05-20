package com.yahoo.searchdefinition;

import com.yahoo.config.application.api.ApplicationPackage;
import com.yahoo.path.Path;
import com.yahoo.vespa.model.AbstractService;
import com.yahoo.vespa.model.utils.FileSender;

import java.util.Collection;

public class RankExpressionFile {

    /** The search definition-unique name of this expression */
    private final String name;
    private final String path;
    private String fileReference = "";

    public RankExpressionFile(String name, String fileName) {
        this.name = name;
        this.path = fileName;
        validate();
    }


    /** Initiate sending of this constant to some services over file distribution */
    public void sendTo(Collection<? extends AbstractService> services) {
        /*
         *  TODO This is a very dirty hack due to using both SEARCH_DEFINITIONS_DIR and SCHEMA_DIR
         *  and doing so inconsistently, combined with using both fields from application package on disk and in zookeeper.
         *  The mess is spread out nicely, but ZookeeperClient, and writeSearchDefinitions and ZkApplicationPackage and FilesApplicationPackage
         *  should be consolidated
        */
        try {
            fileReference = FileSender.sendFileToServices(ApplicationPackage.SCHEMAS_DIR + "/" + path, services).value();
        } catch (IllegalArgumentException e1) {
            try {
                fileReference = FileSender.sendFileToServices(ApplicationPackage.SEARCH_DEFINITIONS_DIR + "/" + path, services).value();
            } catch (IllegalArgumentException e2) {
                throw new IllegalArgumentException("Failed to find expression file '" + path + "' in '"
                        + ApplicationPackage.SEARCH_DEFINITIONS_DIR + "' or '" + ApplicationPackage.SCHEMAS_DIR + "'.", e2);
            }
        }
    }

    public String getName() { return name; }
    public String getFileName() { return path; }
    public Path getFilePath() { return Path.fromString(path); }
    public String getUri() { return path; }
    public String getFileReference() { return fileReference; }

    public void validate() {
        if (path == null || path.isEmpty())
            throw new IllegalArgumentException("Ranking expression must have a file.");
    }

    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("expression '").append(name)
                .append("' from file '").append(path)
                .append("' with ref '").append(fileReference)
                .append("'");
        return b.toString();
    }
}
