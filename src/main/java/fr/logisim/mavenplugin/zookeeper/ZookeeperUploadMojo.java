package fr.logisim.mavenplugin.zookeeper;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.io.File;

@org.apache.maven.plugins.annotations.Mojo(name = "zookeeper-upload")
public class ZookeeperUploadMojo extends AbstractMojo {
    @Parameter(property = "project", readonly = true)
    private MavenProject project;
    @Parameter(property = "inputFile", required = true)
    private File inputFile;
    @Parameter(property = "targetPath", required = true)
    private String targetPath;
    @Parameter(property = "connectionString", required = true)
    private String connectionString;
    @Parameter(property = "sessionTimeout", required = false)
    private int sessionTimeout = 30000;

    @Override
    public void execute() throws MojoFailureException {
        if (inputFile == null || !inputFile.exists())
            throw new MojoFailureException("The input file doesn't exist");
        try (ZooKeeper zk = new ZooKeeper(connectionString, sessionTimeout)) {
            zk.upload(inputFile, targetPath);
        } catch (Exception e) {
            throw new MojoFailureException(e.getMessage(), e);
        }
    }
}
