package org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.task.QueryTask;
import org.bigdatacenter.healthcarecdcdataextractor.domain.transaction.TrRequestInfo;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
public class ExtractionRequest implements Serializable {
    private String databaseName;
    private TrRequestInfo requestInfo;
    private List<QueryTask> queryTaskList;
}
