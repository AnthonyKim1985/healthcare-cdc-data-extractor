package org.bigdatacenter.healthcarecdcdataextractor.service;


import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.task.creation.TableCreationTask;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.task.extraction.DataExtractionTask;

public interface RawDataDBService {
    void extractData(DataExtractionTask dataExtractionTask);

    void createTable(TableCreationTask tableCreationTask);
}
