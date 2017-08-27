package org.bigdatacenter.healthcarecdcdataextractor.resolver.extraction;

import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.parameter.map.ParameterKey;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.parameter.map.ParameterValue;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.ExtractionRequest;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.parameter.ExtractionRequestParameter;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.task.QueryTask;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.task.creation.TableCreationTask;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.task.extraction.DataExtractionTask;
import org.bigdatacenter.healthcarecdcdataextractor.domain.transaction.TrRequestInfo;
import org.bigdatacenter.healthcarecdcdataextractor.resolver.extraction.parameter.ExtractionRequestParameterResolver;
import org.bigdatacenter.healthcarecdcdataextractor.resolver.query.select.SelectClauseBuilder;
import org.bigdatacenter.healthcarecdcdataextractor.resolver.query.where.WhereClauseBuilder;
import org.bigdatacenter.healthcarecdcdataextractor.util.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class ExtractionRequestResolverImpl implements ExtractionRequestResolver {
    private static final Logger logger = LoggerFactory.getLogger(ExtractionRequestResolverImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    private final SelectClauseBuilder selectClauseBuilder;

    private final WhereClauseBuilder whereClauseBuilder;

    private final ExtractionRequestParameterResolver extractionRequestParameterResolver;

    @Autowired
    public ExtractionRequestResolverImpl(SelectClauseBuilder selectClauseBuilder,
                                         WhereClauseBuilder whereClauseBuilder,
                                         ExtractionRequestParameterResolver extractionRequestParameterResolver)
    {
        this.selectClauseBuilder = selectClauseBuilder;
        this.whereClauseBuilder = whereClauseBuilder;
        this.extractionRequestParameterResolver = extractionRequestParameterResolver;
    }

    @Override
    public ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter) {
        if (extractionParameter == null)
            throw new NullPointerException(String.format("%s - extractionParameter is null.", currentThreadName));

        try {
            final TrRequestInfo requestInfo = extractionParameter.getRequestInfo();
            final Integer dataSetUID = requestInfo.getDataSetUID();
            final String databaseName = extractionParameter.getDatabaseName();

            final ExtractionRequestParameter extractionRequestParameter = extractionRequestParameterResolver.buildRequestParameter(extractionParameter);
            final Map<Integer/* Year */, Map<ParameterKey, List<ParameterValue>>> yearParameterMap = extractionRequestParameter.getYearParameterMap();

            final List<QueryTask> queryTaskList = new ArrayList<>();

            //
            // TODO: 1. 임시 테이블 생성과 데이터 추출을 위한 쿼리를 생성한다.
            //
            for (Integer year : yearParameterMap.keySet()) {
                Map<ParameterKey, List<ParameterValue>> parameterMap = yearParameterMap.get(year);

                for (ParameterKey parameterKey : parameterMap.keySet()) {
                    final String tableName = parameterKey.getTableName();
                    final String header = parameterKey.getHeader();

                    //
                    // TODO: 1.1. 임시 테이블 생성을 위한 쿼리를 만든다.
                    //
                    final String selectClause = selectClauseBuilder.buildClause(databaseName, tableName, header, Boolean.FALSE);
                    final String whereClause = whereClauseBuilder.buildClause(parameterMap.get(parameterKey));
                    final String creationQuery = String.format("%s %s", selectClause, whereClause);
                    logger.info(String.format("%s - query: %s", currentThreadName, creationQuery));

                    final String extrDbName = String.format("%s_extracted", databaseName);
                    final String extrTableName = String.format("%s_%s", databaseName, CommonUtil.getHashedString(creationQuery));
                    final String dbAndHashedTableName = String.format("%s.%s", extrDbName, extrTableName);
                    logger.info(String.format("%s - dbAndHashedTableName: %s", currentThreadName, dbAndHashedTableName));

                    TableCreationTask tableCreationTask = new TableCreationTask(dbAndHashedTableName, creationQuery);

                    //
                    // TODO: 1.2. 데이터 추출을 위한 쿼리를 만든다.
                    //
                    final String hdfsLocation = CommonUtil.getHdfsLocation(String.format("%s.%s", databaseName, tableName), dataSetUID);
                    logger.info(String.format("%s - hdfsLocation: %s", currentThreadName, hdfsLocation));

                    final String extractionQuery = selectClauseBuilder.buildClause(extrDbName, extrTableName, header, Boolean.FALSE);
                    logger.info(String.format("%s - extractionQuery: %s", currentThreadName, extractionQuery));

                    DataExtractionTask dataExtractionTask = new DataExtractionTask(tableName, hdfsLocation, extractionQuery, header);

                    queryTaskList.add(new QueryTask(tableCreationTask, dataExtractionTask));
                }
            }
            return new ExtractionRequest(databaseName, requestInfo, queryTaskList);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }
}