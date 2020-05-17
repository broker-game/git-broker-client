package com.github.broker;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.AbortedByHookException;
import org.eclipse.jgit.api.errors.CanceledException;
import org.eclipse.jgit.api.errors.ConcurrentRefUpdateException;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidConfigurationException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.api.errors.NoFilepatternException;
import org.eclipse.jgit.api.errors.NoHeadException;
import org.eclipse.jgit.api.errors.NoMessageException;
import org.eclipse.jgit.api.errors.RefNotAdvertisedException;
import org.eclipse.jgit.api.errors.RefNotFoundException;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.api.errors.UnmergedPathsException;
import org.eclipse.jgit.api.errors.WrongRepositoryStateException;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.PushResult;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.StreamSupport;

@Slf4j
public class GitClientWrapper {

    //Git repository
    private Git git;

    /**
     * Clone a repository
     *
     * @param file file
     * @param repository repository
     * @param branch branch
     */
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

    /**
     * Upgrade current repository
     *
     * @param branch branch
     */
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
            NoHeadException e) {
            LOGGER.warn(e.getLocalizedMessage());
        }catch (RefNotAdvertisedException e) {
            LOGGER.info("Waiting for Event in : {}", branch);
            LOGGER.warn(e.getLocalizedMessage());
        } catch (GitAPIException e) {
            LOGGER.warn(e.getLocalizedMessage());
        }
    }

    /**
     * Add File
     *
     * @param file file
     * @param fileName filename
     * @param content content
     * @param fullName fullName
     * @param email email
     */
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

    /**
     * Push
     * @param user user
     * @param password password
     */
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
