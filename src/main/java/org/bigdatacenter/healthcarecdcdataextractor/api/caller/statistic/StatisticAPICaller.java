package org.bigdatacenter.healthcarecdcdataextractor.api.caller.statistic;

public interface StatisticAPICaller {
    /*
    주소:  http://210.115.182.217:8000/stat/
           파라미터는 dataSetUID, DB, TABLE
     */
    void callCreateStatistic(Integer dataSetUID, String databaseName, String tableName);
}