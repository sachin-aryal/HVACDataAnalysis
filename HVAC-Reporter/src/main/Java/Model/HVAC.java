package Model;

/**
 * Created by iam on 7/2/16.
 */
public class HVAC {

    int buildingId;


    String manager;
    String modalNo;
    String recordDate;
    String recordTime;
    double expectedData;
    double resultedData;


    public HVAC() {
    }

    public HVAC(int buildingId,String manager,String modalNo, String recordDate, String recordTime, double expectedData, double resultedData) {
        this.buildingId = buildingId;
        this.manager=manager;
        this.modalNo = modalNo;
        this.recordDate = recordDate;

        this.recordTime = recordTime;
        this.expectedData = expectedData;
        this.resultedData = resultedData;
    }


    public String getManager() {
        return manager;
    }

    public void setManager(String manager) {
        this.manager = manager;
    }

    public int getBuildingId() {
        return buildingId;
    }

    public void setBuildingId(int buildingId) {
        this.buildingId = buildingId;
    }

    public String getModalNo() {
        return modalNo;
    }

    public void setModalNo(String modalNo) {
        this.modalNo = modalNo;
    }

    public String getRecordDate() {
        return recordDate;
    }

    public void setRecordDate(String recordDate) {
        this.recordDate = recordDate;
    }

    public String getRecordTime() {
        return recordTime;
    }

    public void setRecordTime(String recordTime) {
        this.recordTime = recordTime;
    }

    public double getExpectedData() {
        return expectedData;
    }

    public void setExpectedData(double expectedData) {
        this.expectedData = expectedData;
    }

    public double getResultedData() {
        return resultedData;
    }

    public void setResultedData(double resultedData) {
        this.resultedData = resultedData;
    }
}
