package org.bigdatacenter.healthcarecdcdataextractor.resolver.extraction;

import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.parameter.info.AdjacentTableInfo;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.parameter.map.ParameterKey;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.parameter.map.ParameterValue;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.ExtractionRequest;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.parameter.ExtractionRequestParameter;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.query.JoinParameter;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.task.QueryTask;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.task.creation.TableCreationTask;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.task.extraction.DataExtractionTask;
import org.bigdatacenter.healthcarecdcdataextractor.domain.transaction.TrRequestInfo;
import org.bigdatacenter.healthcarecdcdataextractor.resolver.extraction.parameter.ExtractionRequestParameterResolver;
import org.bigdatacenter.healthcarecdcdataextractor.resolver.query.join.JoinClauseBuilder;
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
import java.util.Set;

@Component
@Qualifier("extractionRequestResolverForKoges")
public class ExtractionRequestResolverImplForKoges implements ExtractionRequestResolver {
    private static final Logger logger = LoggerFactory.getLogger(ExtractionRequestResolverImplForKoges.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    private final SelectClauseBuilder selectClauseBuilder;

    private final WhereClauseBuilder whereClauseBuilder;

    private final JoinClauseBuilder joinClauseBuilder;

    private final ExtractionRequestParameterResolver extractionRequestParameterResolver;

    @Autowired
    public ExtractionRequestResolverImplForKoges(SelectClauseBuilder selectClauseBuilder,
                                                 WhereClauseBuilder whereClauseBuilder,
                                                 JoinClauseBuilder joinClauseBuilder,
                                                 ExtractionRequestParameterResolver extractionRequestParameterResolver) {
        this.selectClauseBuilder = selectClauseBuilder;
        this.whereClauseBuilder = whereClauseBuilder;
        this.joinClauseBuilder = joinClauseBuilder;
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
            final String joinCondition = requestInfo.getJoinCondition();

            final ExtractionRequestParameter extractionRequestParameter = extractionRequestParameterResolver.buildRequestParameter(extractionParameter);

            final Map<Integer/* Year */, Map<ParameterKey, List<ParameterValue>>> yearParameterMap = extractionRequestParameter.getYearParameterMap();
            final Map<Integer/* Year */, Set<AdjacentTableInfo>> yearAdjacentTableInfoMap = extractionRequestParameter.getYearAdjacentTableInfoMap();

            final List<QueryTask> queryTaskList = new ArrayList<>();

            //
            // TODO: 1. 임시 테이블 생성과 데이터 추출을 위한 쿼리를 생성한다.
            //
            for (Integer year : yearParameterMap.keySet()) {
                Map<ParameterKey, List<ParameterValue>> parameterMap = yearParameterMap.get(year);

                Set<ParameterKey> parameterKeySet = parameterMap.keySet();
                if (parameterKeySet.size() != 1)
                    throw new RuntimeException(String.format("%s - Invalid parameter size: The koges dataset can only be filtered by epidata_merge. " +
                            "Please check the affy5_snp column information.", currentThreadName));

                JoinParameter sourceJoinParameter = null;
                for (ParameterKey parameterKey : parameterKeySet) {
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

                    queryTaskList.add(new QueryTask(tableCreationTask, null));
                    sourceJoinParameter = new JoinParameter(extrDbName, extrTableName, header, joinCondition);
                }

                //
                // TODO: 1.2. 조인연산 수행을 위한 쿼리를 만든다.
                //
                Set<AdjacentTableInfo> adjacentTableInfoSet = yearAdjacentTableInfoMap.get(year);
                if (adjacentTableInfoSet.size() != 1)
                    throw new RuntimeException(String.format("%s - Invalid parameter size: The koges adjacent table dataset can only have one item. " +
                            "Please check the exclusive adjacent table variable at integration platform.", currentThreadName));

                JoinParameter targetJoinParameter;
                for (AdjacentTableInfo adjacentTableInfo : adjacentTableInfoSet) {
                    final String tableName = adjacentTableInfo.getTableName();
                    final String header = adjacentTableInfo.getHeader();

                    targetJoinParameter = new JoinParameter(databaseName, tableName, header, "individual_id");

                    final String joinQuery = joinClauseBuilder.buildClause(sourceJoinParameter, targetJoinParameter, Boolean.TRUE);
                    final String joinDbName = String.format("%s_join_%s_integrated", databaseName, joinCondition);
                    final String joinTableName = String.format("%s_%s", databaseName, CommonUtil.getHashedString(joinQuery));
                    final String dbAndHashedTableName = String.format("%s.%s", joinDbName, joinTableName);
                    TableCreationTask tableCreationTask = new TableCreationTask(dbAndHashedTableName, joinQuery);

                    final String snpRs = requestInfo.getSnpRs();
                    final Integer affy5MapNumber = requestInfo.getAffy5MapNumber();
                    final String headerForExtraction = String.format("%s,%s_1,%s_2", header, snpRs, snpRs);
                    final String extractionQuery = selectClauseBuilder.buildClause(joinDbName, joinTableName, headerForExtraction, snpRs, affy5MapNumber);

                    DataExtractionTask dataExtractionTask = new DataExtractionTask(tableName/*Data File Name*/, CommonUtil.getHdfsLocation(dbAndHashedTableName, dataSetUID), extractionQuery, headerForExtraction);

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