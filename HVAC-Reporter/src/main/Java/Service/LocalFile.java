package Service;

import java.io.File;

/**
 * Created by iam on 7/2/16.
 */
public class LocalFile extends AbstractFileFetcher {

    @Override
    public File getFile(String classifier) {
        if(classifier.equalsIgnoreCase("noDrill")) {
            inputFile = new File("/home/iam/IdeaProjects/HVAC-Full-System/HVAC-Reporter/data/secondResult.csv");
        }else{
            inputFile = new File("/home/iam/IdeaProjects/HVAC-Full-System/HVAC-Reporter/data/firstResult.csv");
        }
        return inputFile;
    }
}
