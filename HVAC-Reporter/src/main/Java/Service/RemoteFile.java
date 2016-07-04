package Service;

import org.apache.hadoop.fs.Path;

import java.io.File;

/**
 * Created by iam on 7/2/16.
 */

public class RemoteFile extends AbstractFileFetcher {

    private Path remotePath;

    @Override
    public File getFile(String classifier) {
        Path filePath = null;
        if(classifier.equalsIgnoreCase("drill")){
            filePath = new Path("hdfs://localhost:9000/user/iam/input/firstResult.csv");
        }else{
            filePath = new Path("hdfs://localhost:9000/user/iam/secondResult.csv/part-r-00000");
        }
        setRemotePath(filePath);
        return inputFile;
    }

    public Path getRemotePath() {
        return remotePath;
    }

    public void setRemotePath(Path remotePath) {
        this.remotePath = remotePath;
    }


}
