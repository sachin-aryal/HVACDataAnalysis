package Service;

import java.io.IOException;
import java.io.InputStream;
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
        String remoteUrl = "hadoop-url";
        URL url = null;
        try {
            url = new URL("http://merohostelll.com");
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            if(conn.getResponseCode()==200){
//                return true;
                logger.info("Successfully Connected: "+remoteUrl);
                return false;
            }
        } catch (MalformedURLException e) {
            logger.warning("Malformed URL Found.");
        } catch (IOException e) {
            logger.warning("Input Stream Failed.");
        }
        logger.warning("Unable to Connect:"+remoteUrl);
        return false;
    }

}
