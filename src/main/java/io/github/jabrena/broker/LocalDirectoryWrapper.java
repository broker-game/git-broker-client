package io.github.jabrena.broker;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

@Slf4j
public class LocalDirectoryWrapper {

    //Local File System
    private File localFS;

    /**
     * Create the local directory to clone a Git repository
     *
     */
    public void createLocalRepository() {
        try {
            this.localFS = prepareFolderForGit();
        } catch (IOException e) {
            throw new RuntimeException(e.getLocalizedMessage(), e);
        }
    }

    private File prepareFolderForGit() throws IOException {
        File localPath = File.createTempFile("BROKER_CLIENT" + "_", "");
        if (!localPath.delete()) {
            throw new IOException("Could not delete temporary file " + localPath);
        }
        LOGGER.debug("Creating local directory in: {}", localPath.getAbsolutePath());
        return localPath;
    }

    public File getLocalFS() {
        return localFS;
    }
}
