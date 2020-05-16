package com.github.broker;

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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
public class BrokerClient {

    //Local File System
    private File localFS;

    //Git repository
    private Git git;

    //Branch
    private final String application;

    //Node
    private final String node;

    //Credentials
    private final String broker;
    private final String user;
    private final String password;
    private final String fullName;
    private final String email;

    /**
     * Constructor
     *
     * @param broker      broker
     * @param application application
     * @param node        node
     * @param fullName    fullName
     * @param email       email
     * @param user        user
     * @param password    password
     */
    public BrokerClient(String broker, String application, String node,
                        String fullName, String email, String user, String password) {
        this.broker = broker;
        this.application = application;
        this.node = node;
        this.fullName = fullName;
        this.email = email;
        this.user = user;
        this.password = password;
    }

    /**
     * Constructor
     *
     * @param config ConfigFile
     */
    public BrokerClient(BrokerClientConfig config) {
        this(
            config.getBroker(),
            config.getApplication(),
            config.getNode(),
            config.getFullName(),
            config.getEmail(),
            config.getUser(),
            config.getPassword()
        );
    }

    /**
     * Connect with repository
     *
     * @return result
     */
    public boolean connect() {

        try {
            this.localFS = this.prepareFolderForGit();
            this.git = Git.cloneRepository()
                .setURI(broker)
                .setDirectory(this.localFS)
                //.setBranchesToClone(singleton("refs/heads/master"))
                .setProgressMonitor(new SimpleProgressMonitor())
                .call();

            git.checkout()
                .setCreateBranch(true)
                .setName(this.application)
                .call();

            return true;
        } catch (GitAPIException | IOException e) {
            LOGGER.warn(e.getLocalizedMessage(), e);
            return false;
        }
    }

    /**
     * Produce
     *
     * @param event event
     * @param message message
     * @return
     */
    public boolean produce(String event, Object message) {

        upgradeRepository();
        final String fileName = this.getFilename(event);
        addFile(fileName, message.toString());
        push();

        return true;
    }

    private void upgradeRepository() {
        try {
            git.fetch().setForceUpdate(true).setRemote("origin").call();
            git.pull().setRemoteBranchName(this.application).setProgressMonitor(new SimpleProgressMonitor()).call();
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

    private String getFilename(String event) {
        final String fileName = getEpoch() + "_" + this.node + "_" + event + ".json";
        LOGGER.info(fileName);
        return fileName;
    }

    private long getEpoch() {
        return System.currentTimeMillis();
    }

    private void addFile(String fileName, String content) {

        try {
            Files.writeString(this.localFS.toPath().resolve(fileName), content);
            git.add().addFilepattern(fileName).call();
            git.commit()
                .setMessage("Creating file: " + fileName)
                .setAuthor(this.fullName, this.email)
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

    private void push() {

        try {
            CredentialsProvider cp = new UsernamePasswordCredentialsProvider(this.user, this.password);
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

    /**
     * Consume
     *
     * @param event event
     * @param poolingPeriod Pooling period
     * @return response
     */
    public BrokerResponse consume(String event, int poolingPeriod) {

        getInfiniteStream()
            .map(x -> {
                sleep(poolingPeriod);
                upgradeRepository();
                return x;
            })
            .map(x -> {

                Arrays.stream(this.localFS.list())
                    .filter(y -> y.indexOf(".json") != -1)
                    //.peek(System.out::println)
                    .map(BrokerFileParser::new)
                    .filter(b -> b.getEvent().equals(event))
                    .forEach(System.out::println);

                if (x > 1) {
                    return null;
                }
                return x;
            })
            .peek(System.out::println)
            .takeWhile(Objects::nonNull)
            .count();

        final String fileName = this.getFilename("OK");
        addFile(fileName, "PROCESSED");
        push();

        return new BrokerResponse();
    }

    private Stream<Integer> getInfiniteStream() {
        return Stream.iterate(0, i -> i + 1);
    }

    @SneakyThrows
    private void sleep(int seconds) {
        Thread.sleep(seconds * 1000);
    }

    private File prepareFolderForGit() throws IOException {
        File localPath = File.createTempFile("BROKER_CLIENT" + "_" + this.node + "_", "");
        if (!localPath.delete()) {
            throw new IOException("Could not delete temporary file " + localPath);
        }
        System.out.println(localPath.getAbsolutePath());
        return localPath;
    }

    /**
     * Close
     */
    public void close() {

        if (Objects.nonNull(this.localFS)) {
            try {
                Files.walk(this.localFS.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);

                assert (!this.localFS.exists());
            } catch (IOException e) {
                LOGGER.warn(e.getLocalizedMessage(), e);
            }
        }
    }
}
