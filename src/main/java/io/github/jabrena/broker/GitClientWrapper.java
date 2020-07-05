package io.github.jabrena.broker;

import lombok.SneakyThrows;
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
import org.eclipse.jgit.api.errors.RefAlreadyExistsException;
import org.eclipse.jgit.api.errors.RefNotAdvertisedException;
import org.eclipse.jgit.api.errors.RefNotFoundException;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.api.errors.UnmergedPathsException;
import org.eclipse.jgit.api.errors.WrongRepositoryStateException;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.PushResult;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Set;

@Slf4j
public class GitClientWrapper {

    private Git git;
    private String repository;
    private File localDirectory;
    private Authentication authentication;

    /**
     * Clone a repository
     *
     * @param file       file
     * @param repository repository
     */
    public void cloneRepository(File file, String repository) {

        this.localDirectory = file;
        this.repository = repository;

        try {
            LOGGER.debug("Cloning repository: {}", repository);
            git = Git.cloneRepository()
                .setURI(repository)
                .setDirectory(file)
                .call();
        } catch (RefNotFoundException e) {
            LOGGER.warn("Empty repository");
            LOGGER.warn(e.getLocalizedMessage());
        } catch (GitAPIException e) {
            LOGGER.warn(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Checkout
     *
     * @param branch Git Branch
     */
    public void checkout(String branch) {
        try {
            LOGGER.debug("Switching to branch: {}", branch);
            git.checkout()
                .setCreateBranch(true)
                .setName(branch)
                .call();
        } catch (RefAlreadyExistsException e) {
            LOGGER.warn(e.getLocalizedMessage());
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
            git.pull().setRemoteBranchName(branch).call();

            //showLogs();

        } catch (WrongRepositoryStateException |
            InvalidConfigurationException |
            CanceledException |
            InvalidRemoteException |
            TransportException |
            RefNotFoundException |
            NoHeadException e) {
            LOGGER.warn(e.getLocalizedMessage(), e);
        } catch (RefNotAdvertisedException e) {
            LOGGER.info("Waiting for Event in : {}", branch);
            LOGGER.trace(e.getLocalizedMessage(), e);
        } catch (GitAPIException e) {
            LOGGER.warn(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Add File
     *
     * @param file     file
     * @param fileName filename
     * @param content  content
     */
    public String addFile(File file, String fileName, String content) {

        String id = "";
        try {
            Files.writeString(file.toPath().resolve(fileName), content);
            git.add()
                .addFilepattern(fileName)
                .call();

            RevCommit rev = git.commit()
                .setMessage("Creating file: " + fileName)
                .setAuthor(this.authentication.getFullName(), this.authentication.getEmail())
                .call();

            id = rev.getId().getName();
        } catch (UnmergedPathsException | WrongRepositoryStateException | AbortedByHookException |
            NoMessageException | NoFilepatternException | NoHeadException | ConcurrentRefUpdateException |
            IOException e) {

            LOGGER.warn(e.getLocalizedMessage(), e);
        } catch (GitAPIException e) {
            LOGGER.warn(e.getLocalizedMessage(), e);
        }

        return id;
    }

    /**
     * Remove a file
     * @param fileName fileName
     */
    public void removeFile(String fileName) {
        try {

            git.rm()
                .addFilepattern(fileName)
                .call();

            git.commit()
                .setMessage("Removing file: " + fileName)
                .setAuthor(this.authentication.getFullName(), this.authentication.getEmail())
                .call();

        } catch (GitAPIException e) {
            LOGGER.warn(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Push
     */
    public void push() {

        try {

            CredentialsProvider cp = new UsernamePasswordCredentialsProvider(
                this.authentication.getUser(), this.authentication.getPassword());

            Iterable<PushResult> results = git.push()
                .setRemote("origin")
                .setCredentialsProvider(cp)
                .call();

        } catch (InvalidRemoteException | TransportException e) {
            LOGGER.warn(e.getLocalizedMessage(), e);
        } catch (GitAPIException e) {
            LOGGER.warn(e.getLocalizedMessage(), e);
        }
    }

    //TODO Review
    @SneakyThrows
    private void showLogs() {

        var count = 0;
        Map<AnyObjectId, Set<Ref>> allsRefs = git.getRepository().getAllRefsByPeeledObjectId();
        Iterable<RevCommit> commits = git.log().call();
        for (RevCommit commit : commits) {
            LOGGER.info("commit msg={}", commit.getShortMessage());
            count++;
            Set<Ref> commitRefs = allsRefs.get(commit);
            if (commitRefs == null) {
                continue;
            }
            for (Ref ref : commitRefs) {

                String name = ref.getName();
                LOGGER.info("    rev={}", name);
            }
        }
        LOGGER.info("{}", count);

    }

    public void setAuthentication(Authentication authentication) {
        this.authentication = authentication;
    }

}
