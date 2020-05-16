package com.github.broker;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.*;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.PushResult;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.StreamSupport;

@Slf4j
public class GitWrapper {

    //Git repository
    private Git git;

    public void cloneRepository(File file, String repository, String branch) {

        try {
            git = Git.cloneRepository()
                .setURI(repository)
                .setDirectory(file)
                //.setBranchesToClone(singleton("refs/heads/master"))
                .setProgressMonitor(new SimpleProgressMonitor())
                .call();

            git.checkout()
                .setCreateBranch(true)
                .setName(branch)
                .call();

        } catch (GitAPIException e) {
            LOGGER.warn(e.getLocalizedMessage(), e);
        }
    }

    public void upgradeRepository(String branch) {
        try {
            git.fetch().setForceUpdate(true).setRemote("origin").call();
            git.pull().setRemoteBranchName(branch).setProgressMonitor(new SimpleProgressMonitor()).call();
        } catch (WrongRepositoryStateException |
            InvalidConfigurationException |
            CanceledException |
            InvalidRemoteException |
            TransportException |
            RefNotFoundException |
            NoHeadException |
            RefNotAdvertisedException e) {

            LOGGER.warn(e.getLocalizedMessage());
        } catch (GitAPIException e) {
            LOGGER.warn(e.getLocalizedMessage());
        }
    }

    public void addFile(File file, String fileName, String content, String fullName, String email) {

        try {
            Files.writeString(file.toPath().resolve(fileName), content);
            git.add().addFilepattern(fileName).call();
            git.commit()
                .setMessage("Creating file: " + fileName)
                .setAuthor(fullName, email)
                .call();
        } catch (UnmergedPathsException |
            WrongRepositoryStateException |
            AbortedByHookException |
            NoMessageException |
            NoFilepatternException |
            NoHeadException |
            ConcurrentRefUpdateException |
            IOException e) {

            LOGGER.warn(e.getLocalizedMessage(), e);
        } catch (GitAPIException e) {
            LOGGER.warn(e.getLocalizedMessage(), e);
        }
    }

    public void push(String user, String password) {

        try {
            CredentialsProvider cp = new UsernamePasswordCredentialsProvider(user, password);
            Iterable<PushResult> results = git.push()
                .setRemote("origin")
                .setCredentialsProvider(cp)
                .call();

            StreamSupport.stream(results.spliterator(), false)
                .forEach(result -> {
                    LOGGER.info(result.getMessages());
                });
        } catch (InvalidRemoteException | TransportException e) {
            LOGGER.warn(e.getLocalizedMessage(), e);
        } catch (GitAPIException e) {
            LOGGER.warn(e.getLocalizedMessage(), e);
        }
    }
}
