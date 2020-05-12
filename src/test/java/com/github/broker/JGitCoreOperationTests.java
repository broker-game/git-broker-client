package com.github.broker;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.ProgressMonitor;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Collections.singleton;

@Slf4j
public class JGitCoreOperationTests {

    private final String AUTHOR = "Juan Antonio Breña Moral";
    private final String EMAIL = "bren@juanantonio.info";
    private final String REMOTE_URL = "https://github.com/ev3dev-lang-java/broker-dev-environment";
    private final String BRANCH_NAME = "branch2";
    private final String GIT_USER = "xxx";
    private final String GIT_PASSWORD = "yyy";

    @Test
    public void given_LocalRepository_when_clone_then_Ok() throws Exception {

        final File localPath;
        try (Repository repository = createNewRepository()) {
            localPath = repository.getWorkTree();

            try (Git git = new Git(repository)) {

                final String fileName = "file1.md";
                addFile(localPath, git, fileName);
            }
        }

        // clean up here to not keep using more and more disk-space for these samples
        FileUtils.deleteDirectory(localPath);
    }

    @Test
    public void given_RemoteRepository_when_clone_then_Ok() throws Exception {

        final File localPath = prepareFolderForGit();

        try (Git git = Git.cloneRepository()
            .setURI(REMOTE_URL)
            .setDirectory(localPath)
            .setBranchesToClone(singleton("refs/heads/master"))
            .setProgressMonitor(new SimpleProgressMonitor())
            .call()) {

            final String fileName = "file1.md";
            addFile(localPath, git, fileName);
        }

        // clean up here to not keep using more and more disk-space for these samples
        FileUtils.deleteDirectory(localPath);
    }

    @Test
    public void given_RemoteRepository_when_fetchPull_then_Ok() throws Exception {

        final File localPath = prepareFolderForGit();

        try (Git git = Git.cloneRepository()
            .setURI(REMOTE_URL)
            .setDirectory(localPath)
            .setBranchesToClone(singleton("refs/heads/master"))
            .setProgressMonitor(new SimpleProgressMonitor())
            .call()) {

            git.fetch().setRemote("origin").call();
            git.pull().call();
        }

        // clean up here to not keep using more and more disk-space for these samples
        FileUtils.deleteDirectory(localPath);
    }

    @Test
    public void given_RemoteRepository_when_createBranch_then_Ok() throws Exception {

        final File localPath = prepareFolderForGit();

        try (Git git = Git.cloneRepository()
            .setURI(REMOTE_URL)
            .setDirectory(localPath)
            .setBranchesToClone(singleton("refs/heads/master"))
            .setProgressMonitor(new SimpleProgressMonitor())
            .call()) {

            git.checkout()
                .setName(BRANCH_NAME)
                .setCreateBranch(true)
                .call();
        }

        // clean up here to not keep using more and more disk-space for these samples
        FileUtils.deleteDirectory(localPath);
    }

    @Disabled
    @Test
    public void given_RemoteRepository_when_push_then_Ok() throws Exception {

        final File localPath = prepareFolderForGit();

        try (Git git = Git.cloneRepository()
            .setURI(REMOTE_URL)
            .setDirectory(localPath)
            .setBranchesToClone(singleton("refs/heads/master"))
            .setProgressMonitor(new SimpleProgressMonitor())
            .call()) {

            git.checkout()
                .setName(BRANCH_NAME)
                .setCreateBranch(true)
                .call();

            final String fileName = "file3.md";
            addFile(localPath, git, fileName);

            push(git);
        }

        // clean up here to not keep using more and more disk-space for these samples
        FileUtils.deleteDirectory(localPath);
    }



    private void push(Git git) throws GitAPIException {
        CredentialsProvider cp = new UsernamePasswordCredentialsProvider(GIT_USER, GIT_PASSWORD);
        git.push()
            .setRemote("origin")
            .setCredentialsProvider(cp)
            .call();
    }

    private void addFile(File localPath, Git git, String fileName) throws IOException, GitAPIException {

        Files.deleteIfExists(localPath.toPath().resolve(fileName));
        Files.writeString(localPath.toPath().resolve(fileName), "Hello World 2");
        git.add().addFilepattern(fileName).call();
        git.commit()
            .setMessage("create " + fileName)
            .setAuthor(AUTHOR, EMAIL)
            .call();
    }

    @Test
    public void given_RemoteRepository_when_cloneIterate_then_Ok() throws Exception {

        final File localPath = prepareFolderForGit();

        try (Git git = Git.cloneRepository()
            .setURI(REMOTE_URL)
            .setDirectory(localPath)
            .setBranchesToClone(singleton("refs/heads/master"))
            .setProgressMonitor(new SimpleProgressMonitor())
            .call()) {

            Iterable<RevCommit> logs = git.log().all().call();
            StreamSupport.stream(logs.spliterator(), false).forEach( rev -> {
                System.out.print(Instant.ofEpochSecond(rev.getCommitTime()));
                System.out.print(": ");
                System.out.print(rev.getFullMessage());
                System.out.println();
                System.out.println(rev.getId().getName());
                System.out.print(rev.getAuthorIdent().getName());
                System.out.println(rev.getAuthorIdent().getEmailAddress());
                System.out.println("-------------------------");
            });
        }

        // clean up here to not keep using more and more disk-space for these samples
        FileUtils.deleteDirectory(localPath);
    }

    private static class SimpleProgressMonitor implements ProgressMonitor {

        @Override
        public void start(int totalTasks) {
            LOGGER.debug("Starting work on " + totalTasks + " tasks");
        }

        @Override
        public void beginTask(String title, int totalWork) {
            LOGGER.debug("Start " + title + ": " + totalWork);
        }

        @Override
        public void update(int completed) {
            LOGGER.debug(completed + "-");
        }

        @Override
        public void endTask() {
            LOGGER.debug("Done");
        }

        @Override
        public boolean isCancelled() {
            return false;
        }
    }

    private static File prepareFolderForGit() throws IOException {
        // prepare a new folder
        File localPath = File.createTempFile("TestGitRepository", "");
        if(!localPath.delete()) {
            throw new IOException("Could not delete temporary file " + localPath);
        }
        return localPath;
    }

    private static Repository createNewRepository() throws IOException {


        // create the directory
        Repository repository = FileRepositoryBuilder.create(new File(prepareFolderForGit(), ".git"));
        repository.create();

        return repository;
    }

}
