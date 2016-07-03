package Service;

import java.io.File;

/**
 * Created by iam on 7/2/16.
 */

public abstract class AbstractFileFetcher implements FileFetcher{

    protected File inputFile = null;

    public File getFile(String classifier) {
        return inputFile;
    }
}
