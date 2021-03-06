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
            throw new NullPointerException("The extractionParameter is null.");

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
                    throw new RuntimeException("Invalid parameter size: The koges dataset can only be filtered by epidata_merge. \" +\n" +
                            "                            \"Please check the affy5_snp column information.");

                String headerForEpidata = null;
                JoinParameter sourceJoinParameter = null;

                for (ParameterKey parameterKey : parameterKeySet) {
                    headerForEpidata = parameterKey.getHeader();

                    //
                    // TODO: 1.1. 임시 테이블 생성을 위한 쿼리를 만든다.
                    //
                    final String selectClause = selectClauseBuilder.buildClause(databaseName, parameterKey.getTableName(), headerForEpidata, Boolean.FALSE);
                    final String whereClause = whereClauseBuilder.buildClause(parameterMap.get(parameterKey));
                    final String query = String.format("%s %s", selectClause, whereClause);
                    logger.debug(String.format("(dataSetUID=%d / threadName=%s) - query: %s", dataSetUID, currentThreadName, query));

                    final String extrDbName = String.format("%s_extracted", databaseName);
                    final String extrTableName = String.format("%s_%s", databaseName, CommonUtil.getHashedString(query));
                    final String dbAndHashedTableName = String.format("%s.%s", extrDbName, extrTableName);
                    logger.debug(String.format("(dataSetUID=%d / threadName=%s) - dbAndHashedTableName: %s", dataSetUID, currentThreadName, dbAndHashedTableName));

                    TableCreationTask tableCreationTask = new TableCreationTask(dbAndHashedTableName, query);

                    queryTaskList.add(new QueryTask(tableCreationTask, null));
                    sourceJoinParameter = new JoinParameter(extrDbName, extrTableName, headerForEpidata, joinCondition);
                }

                //
                // TODO: 1.2. 조인연산 수행을 위한 쿼리를 만든다.
                //
                Set<AdjacentTableInfo> adjacentTableInfoSet = yearAdjacentTableInfoMap.get(year);
                if (adjacentTableInfoSet.size() != 1)
                    throw new RuntimeException("Invalid parameter size: The koges adjacent table dataset can only have one item. \" +\n" +
                            "                            \"Please check the exclusive adjacent table variable at integration platform.");

                JoinParameter targetJoinParameter;
                for (AdjacentTableInfo adjacentTableInfo : adjacentTableInfoSet) {
                    final String tableName = adjacentTableInfo.getTableName();
                    final String headerForAdjacentTable = adjacentTableInfo.getHeader();

                    targetJoinParameter = new JoinParameter(databaseName, tableName, headerForAdjacentTable, "individual_id");

                    final String joinQuery = joinClauseBuilder.buildClause(sourceJoinParameter, targetJoinParameter, Boolean.TRUE);
                    final String joinDbName = String.format("%s_join_%s_integrated", databaseName, joinCondition);
                    final String joinTableName = String.format("%s_%s", databaseName, CommonUtil.getHashedString(joinQuery));
                    final String dbAndHashedTableName = String.format("%s.%s", joinDbName, joinTableName);
                    TableCreationTask tableCreationTask = new TableCreationTask(dbAndHashedTableName, joinQuery);
                    queryTaskList.add(new QueryTask(tableCreationTask, null));

                    queryTaskList.addAll(getQueryTaskForRsTransformation(requestInfo, joinDbName, joinTableName, headerForEpidata, year));
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

    private List<QueryTask> getQueryTaskForRsTransformation(TrRequestInfo requestInfo, String snpDbName, String snpTableName, String headerForEpidata, Integer year) {
        final List<QueryTask> queryTaskList = new ArrayList<>();

        final String snpRs = requestInfo.getSnpRs();
        final Integer dataSetUID = requestInfo.getDataSetUID();
        final String affy5MapNumber = requestInfo.getAffy5MapNumber();

        final String rsDbName = "koges_rs_extracted";
        final String dataFileName = String.format("koges_ans_%s", year);

        //
        // TODO: Exclude rows, if the value of edate is '55555'
        //
        final List<ParameterValue> parameterValueList = new ArrayList<>();
        parameterValueList.add(new ParameterValue(1, String.format("a0%d_edate", year), "55555", "<>"));

        final String selectClause;
        final String headerForExtraction;

        if (snpRs == null || affy5MapNumber == null) {
            selectClause = selectClauseBuilder.buildClause(snpDbName, snpTableName, headerForEpidata, Boolean.FALSE);
            headerForExtraction = headerForEpidata;
        } else {
            selectClause = selectClauseBuilder.buildClause(snpDbName, snpTableName, headerForEpidata, snpRs, affy5MapNumber);
            headerForExtraction = getHeaderForExtraction(headerForEpidata, snpRs);
        }

        final String rsQuery = String.format("%s %s", selectClause, whereClauseBuilder.buildClause(parameterValueList));
        final String rsTableName = String.format("%s_%s", rsDbName, CommonUtil.getHashedString(rsQuery));
        final String dbAndHashedTableName = String.format("%s.%s", rsDbName, rsTableName);
        final TableCreationTask tableCreationTask = new TableCreationTask(dbAndHashedTableName, rsQuery);

        final String extractionQuery = selectClauseBuilder.buildClause(rsDbName, rsTableName, headerForExtraction, Boolean.FALSE);
        final DataExtractionTask dataExtractionTask = new DataExtractionTask(dataFileName, CommonUtil.getHdfsLocation(dbAndHashedTableName, dataSetUID), extractionQuery, headerForExtraction);

        queryTaskList.add(new QueryTask(tableCreationTask, dataExtractionTask));

        logger.info(String.format("(dataSetUID=%d / threadName=%s) - QueryTaskList For operation of RS-transformation: %s", dataSetUID, currentThreadName, queryTaskList));

        return queryTaskList;
    }

    private String getHeaderForExtraction(String header, String snpRs) {
        final StringBuilder extractionHeaderBuilder = new StringBuilder(header);
        final String[] snpRsArray = snpRs.split("[,]");

        for (String rs : snpRsArray)
            extractionHeaderBuilder.append(String.format(",%s_1,%s_2", rs, rs));

        return extractionHeaderBuilder.toString();
    }
}