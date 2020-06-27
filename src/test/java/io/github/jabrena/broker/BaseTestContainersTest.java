package io.github.jabrena.broker;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.revwalk.RevCommit;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Testcontainers
abstract class BaseTestContainersTest {

    protected static String BROKER_TEST_ADDRESS;
    private static AtomicBoolean first = new AtomicBoolean(true);

    @Container
    protected GenericContainer<?> container = new GenericContainer<>("jabrena/git-server:latest")
        .withExposedPorts(80)
        .withLogConsumer(new Slf4jLogConsumer(LOGGER).withSeparateOutputStreams())
        .waitingFor(Wait.forLogMessage(".*AH00558: apache2.*", 1));

    @BeforeEach
    public void before() throws IOException, InterruptedException, GitAPIException {

        LOGGER.info("");
        LOGGER.info("*************************************");
        LOGGER.info("Initialize Git repository for testing");

        ExecResult result = container.execInContainer("mkrepo", "test");
        LOGGER.info(result.getStdout());

        var ip = container.getContainerIpAddress();
        var port = container.getMappedPort(80);

        BROKER_TEST_ADDRESS = "http://" + ip + ":" + port + "/test.git";
        LOGGER.info(BROKER_TEST_ADDRESS);

        if (first.get()) {
            createTheFirstCommit();
            first.set(false);
        }

        LOGGER.info("End Git Initialization");
        LOGGER.info("**********************");
        LOGGER.info("");
    }

    private void createTheFirstCommit() throws GitAPIException, IOException {
        LocalDirectoryWrapper localDirectoryWrapper = new LocalDirectoryWrapper();
        localDirectoryWrapper.createLocalRepository();
        localDirectoryWrapper.getLocalFS();

        var workingDirectory = localDirectoryWrapper.getLocalFS();

        Git git = Git.cloneRepository()
            .setURI(BROKER_TEST_ADDRESS)
            .setDirectory(workingDirectory)
            .call();

        String fileName = "myNewFile";
        File newFile = new File(workingDirectory, fileName);
        newFile.createNewFile();
        git.add().addFilepattern(fileName).call();

        RevCommit rev = git.commit()
            .setAuthor("Juan Antonio Breña Moral", "bren@juanantonio.info")
            .setMessage("My first commit")
            .call();

        git.push().call();
    }

}
