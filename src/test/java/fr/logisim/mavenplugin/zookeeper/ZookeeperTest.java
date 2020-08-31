package fr.logisim.mavenplugin.zookeeper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;

import org.junit.jupiter.api.Test;

public class ZookeeperTest {

    private final String CONNECTION_STRING = "localhost";

    @Test
    public void testUpload() throws Exception {
        try (ZooKeeper zk = new ZooKeeper(CONNECTION_STRING, 10000)) {
            zk.delete("/test");
            assertFalse(zk.exists("/test"));
            assertEquals(0, zk.delete(""));

            assertThrows(FileNotFoundException.class, () -> zk.upload(null, "/test/"));
            assertThrows(FileNotFoundException.class, () -> zk.upload(new File("/tmp/notexisting.txt"), "/test/"));

            // Test upload file
            assertEquals(1, zk.upload(getTestFile("file1.txt"), "/test/file1.txt"));
            assertEquals(1, zk.upload(getTestFile("file1.txt"), "/test/"));
            assertTrue(zk.exists("/test"));
            assertTrue(zk.exists("/test/file1.txt"));

            // Test upload directory
            assertEquals(2, zk.upload(getTestFile("directory"), "/test/directory"));
            assertEquals(2, zk.upload(getTestFile("directory"), "/test/directory2/"));

            // Remove
            assertEquals(10, zk.delete("/test"));
        }
    }

    private File getTestFile(final String name) {
        return new File(getClass().getClassLoader().getResource(name).getFile());
    }
}