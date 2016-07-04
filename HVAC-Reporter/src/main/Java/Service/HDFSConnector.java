package Service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Logger;

/**
 * Created by iam on 7/2/16.
 */

public class HDFSConnector {
    static Logger logger = LoggerObject.getLoggerObject();

    public static boolean checkConnection(){
        String remoteUrl = "hdfs://localhost:9000";

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", remoteUrl);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(configuration);
            Path filePath = new Path("hdfs://localhost:9000/user/iam/input/firstResult.csv");
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
            if(reader.ready()){
                logger.info("Successfully Connected: "+remoteUrl);
                return true;
            }

        } catch (IOException e) {
            System.out.println("Failed Connection With Remote Hadoop File System.");
//            e.printStackTrace();
        }
        logger.warning("Unable to Connect:"+remoteUrl);
        return false;
    }

}
