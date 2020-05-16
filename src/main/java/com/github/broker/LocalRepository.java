package com.github.broker;

import java.io.File;
import java.io.IOException;

public class LocalRepository {

    //Local File System
    private File localFS;

    public LocalRepository() {

    }

    public void createLocalRepository(String node) {
        try {
            this.localFS = prepareFolderForGit(node);
        } catch (IOException e) {
            throw new RuntimeException(e.getLocalizedMessage(), e);
        }

    }

    private File prepareFolderForGit(String node) throws IOException {
        File localPath = File.createTempFile("BROKER_CLIENT" + "_" + node + "_", "");
        if (!localPath.delete()) {
            throw new IOException("Could not delete temporary file " + localPath);
        }
        System.out.println(localPath.getAbsolutePath());
        return localPath;
    }

    public File getLocalFS() {
        return localFS;
    }
}
