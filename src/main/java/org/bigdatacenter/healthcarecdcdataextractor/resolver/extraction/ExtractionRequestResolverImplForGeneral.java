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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
@Qualifier("extractionRequestResolverForGeneral")
public class ExtractionRequestResolverImplForGeneral implements ExtractionRequestResolver {
    private static final Logger logger = LoggerFactory.getLogger(ExtractionRequestResolverImplForGeneral.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    private final SelectClauseBuilder selectClauseBuilder;

    private final WhereClauseBuilder whereClauseBuilder;

    private final ExtractionRequestParameterResolver extractionRequestParameterResolver;

    @Autowired
    public ExtractionRequestResolverImplForGeneral(SelectClauseBuilder selectClauseBuilder,
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
            throw new NullPointerException("The extractionParameter is null.");

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
                    final String query = String.format("%s %s", selectClause, whereClause);
                    logger.debug(String.format("(dataSetUID=%d / threadName=%s) - query: %s", dataSetUID, currentThreadName, query));

                    final String extrDbName = String.format("%s_extracted", databaseName);
                    final String extrTableName = String.format("%s_%s", databaseName, CommonUtil.getHashedString(query));
                    final String dbAndHashedTableName = String.format("%s.%s", extrDbName, extrTableName);
                    logger.debug(String.format("(dataSetUID=%d / threadName=%s) - dbAndHashedTableName: %s", dataSetUID, currentThreadName, dbAndHashedTableName));

                    TableCreationTask tableCreationTask = new TableCreationTask(dbAndHashedTableName, query);

                    //
                    // TODO: 1.2. 데이터 추출을 위한 쿼리를 만든다.
                    //
                    final String hdfsLocation = CommonUtil.getHdfsLocation(String.format("%s.%s", databaseName, tableName), dataSetUID);
                    logger.debug(String.format("(dataSetUID=%d / threadName=%s) - hdfsLocation: %s", dataSetUID, currentThreadName, hdfsLocation));

                    final String extractionQuery = selectClauseBuilder.buildClause(extrDbName, extrTableName, header, Boolean.FALSE);
                    logger.debug(String.format("(dataSetUID=%d / threadName=%s) - extractionQuery: %s", dataSetUID, currentThreadName, extractionQuery));

                    DataExtractionTask dataExtractionTask = new DataExtractionTask(tableName, hdfsLocation, extractionQuery, header);
                    queryTaskList.add(new QueryTask(tableCreationTask, dataExtractionTask));
                }
            }

            final ExtractionRequest extractionRequest = new ExtractionRequest(databaseName, requestInfo, queryTaskList);
            logger.info(String.format("(dataSetUID=%d / threadName=%s) - ExtractionRequest: %s", dataSetUID, currentThreadName, extractionRequest));

            return extractionRequest;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }
}