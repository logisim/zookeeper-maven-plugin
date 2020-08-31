package fr.logisim.mavenplugin.zookeeper;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ZooKeeper implements Closeable {

    private final org.apache.zookeeper.ZooKeeper zk;

    /**
     * Constructor for ZK instance
     * 
     * @param connectionString ZK connection string
     * @param sessionTimeout ZK session timeout
     * @throws IOException IO exception
     */
    public ZooKeeper(final String connectionString, final int sessionTimeout) throws IOException {
        zk = new org.apache.zookeeper.ZooKeeper(connectionString, sessionTimeout, event -> {});
    }

    /**
     * Upload file or directory to Zookeeper instance
     * 
     * @param inputFile Input file or directory
     * @param targetPath Target path
     * @return number of files pushed
     * @throws IOException file exception
     * @throws InterruptedException ZK interrupted tranfer error
     * @throws KeeperException ZK exception
     */
    public int upload(final File inputFile, final String targetPath) throws KeeperException, InterruptedException, IOException {
        if (inputFile == null) {
            throw new FileNotFoundException("File is null");
        } else if (inputFile.isFile()) {
            final String path = targetPath.endsWith("/") ? targetPath + inputFile.getName() : targetPath;
            return pushFile(inputFile, path);
        } else if (inputFile.isDirectory()) {
            return pushDirectory(inputFile.toPath(), targetPath);
        } else {
            throw new FileNotFoundException(inputFile.toString());
        }
    }

    private int pushDirectory(final Path inputDir, final String targetPath) throws KeeperException, InterruptedException, IOException {
        try (Stream<Path> stream = Files.walk(inputDir)) {
            List<Path> subfiles = stream.filter(Files::isRegularFile).collect(Collectors.toList());
            int count = 0;
            for (Path path : subfiles) {
                String relative = inputDir.relativize(path).toString().replace('\\', '/');
                count += pushFile(path.toFile(), targetPath + (targetPath.endsWith("/") ? "" : "/") + relative);
            }
            return count;
        }
    }

    private int pushFile(final File inputFile, final String zkFileName) throws KeeperException, InterruptedException, IOException {
        createRecursively(zkFileName);
        final Stat stat = zk.exists(zkFileName, false);
        zk.setData(zkFileName, Files.readAllBytes(inputFile.toPath()), stat == null ? -1 : stat.getVersion());
        return 1;
    }

    public boolean exists(final String path) throws KeeperException, InterruptedException {
        return path.length() > 0 && zk.exists(path, false) != null;
    }

    private void createRecursively(final String path) throws KeeperException, InterruptedException {
        if (path.length() > 0 && zk.exists(path, false) == null) {
            final String temp = path.substring(0, path.lastIndexOf('/'));
            createRecursively(temp);
            zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    public int delete(String path) throws InterruptedException, KeeperException {
        int count = 0;
        if (path.length() == 0)
            return 0;
        final Stat stat = zk.exists(path, false);
        if (stat == null)
            return 0;
        List<String> children = zk.getChildren(path, false);
        for (String child : children) {
            count += delete(path + "/" + child);
        }
        zk.delete(path, stat.getVersion());
        return count + 1;
    }

    @Override
    public void close() throws IOException {
        try {
            zk.close();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

}