package Service;

import Model.HVAC;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by iam on 7/2/16.
 */
public class FetchService {

    Logger logger = LoggerObject.getLoggerObject();

    protected List<HVAC> processedData;

    public void setData(){
        List<HVAC> data = new ArrayList<HVAC>();

        FileFetcher fetcher = getFileFetcherObject();
        File file = fetcher.getFile("noDrill");
        BufferedReader reader = null;
        try {
            reader = getReaderObject(fetcher,file);
            int first = 0;
            HVAC hvac = null;
            String eachLine = "";

            BufferedWriter writer = null;
            if(fetcher instanceof RemoteFile) {
                File f = new File("/home/iam/IdeaProjects/HVAC-Full-System/HVAC-Reporter/data/secondResult.csv");
                f.deleteOnExit();
                writer = new BufferedWriter(new FileWriter(f));
            }
            while ((eachLine=reader.readLine())!=null) {
                if(fetcher instanceof RemoteFile){
                    writer.write(eachLine);
                    writer.newLine();
                }
                if (first != 0) {
                    hvac = new HVAC();
                    String[] separatedData = eachLine.split(",");
                    hvac.setBuildingId(Integer.parseInt(separatedData[0]));
                    hvac.setModalNo(separatedData[1]);
                    hvac.setExpectedData(formatDouble(Double.parseDouble(separatedData[2])));
                    hvac.setResultedData(formatDouble(Double.parseDouble(separatedData[3])));
                    data.add(hvac);
                }
                first++;
            }
            setProcessedData(data);

        } catch (FileNotFoundException e) {
            System.out.println("File not Found.");
        } catch (IOException e) {
            System.out.println("IO Exception.");
        }
    }

    public void setData(int buildingId){
        List<HVAC> data = new ArrayList<HVAC>();
        FileFetcher fetcher = getFileFetcherObject();
        File file = fetcher.getFile("drill");
        BufferedReader reader = null;
        try {
            reader = getReaderObject(fetcher,file);
            HVAC hvac = null;
            String eachLine = "";
            int first = 0;
            BufferedWriter writer = null;
            if(fetcher instanceof RemoteFile) {
                File f = new File("/home/iam/IdeaProjects/HVAC-Full-System/HVAC-Reporter/data/firstResult.csv");
                f.deleteOnExit();
                writer = new BufferedWriter(new FileWriter(f));
            }

            while ((eachLine=reader.readLine())!=null){

                if(fetcher instanceof RemoteFile) {
                    writer.write(eachLine);
                    writer.newLine();
                }

                if(first!=0){
                    String[] separatedData = eachLine.split(",");
                    if(buildingId==Integer.parseInt(separatedData[0])) {
                        hvac = new HVAC();
                        hvac.setBuildingId(Integer.parseInt(separatedData[0]));
                        hvac.setManager(separatedData[1]);
                        hvac.setModalNo(separatedData[2]);
                        hvac.setRecordDate(separatedData[3]);
                        hvac.setRecordTime(separatedData[4]);
                        hvac.setExpectedData(formatDouble(Double.parseDouble(separatedData[5])));
                        hvac.setResultedData(formatDouble(Double.parseDouble(separatedData[6]+"0")));
                        data.add(hvac);
                    }
                }
                first++;
            }

            setProcessedData(data);

        } catch (FileNotFoundException e) {
            System.out.println("File not Found.");
//            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public FileFetcher getFileFetcherObject(){

        if(HDFSConnector.checkConnection()){
            logger.info("Returned Remote File Object.");
            return new RemoteFile();
        }
        logger.info("Returned Local File Object.");
        return new LocalFile();
    }


    public List<HVAC> getProcessedData() {
        return processedData;
    }

    public void setProcessedData(List<HVAC> processedData) {
        this.processedData = processedData;
    }


    public double formatDouble(double val){
        NumberFormat formatter = new DecimalFormat("#0.00");
        return Double.parseDouble(formatter.format(val));
    }

    public BufferedReader getReaderObject(FileFetcher fetcher,File file){
        BufferedReader reader =  null;
        if(fetcher instanceof RemoteFile){
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "hdfs://localhost:9000");
            FileSystem fs = null;
            try {
                fs = FileSystem.get(configuration);
                reader = new BufferedReader(new InputStreamReader(fs.open(((RemoteFile) fetcher).getRemotePath())));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            try {
                reader = new BufferedReader(new FileReader(file));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        return reader;
    }

}
