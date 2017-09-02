package org.bigdatacenter.healthcarecdcdataextractor.rabbitmq;

import org.bigdatacenter.healthcarecdcdataextractor.api.caller.DataIntegrationPlatformAPICaller;
import org.bigdatacenter.healthcarecdcdataextractor.config.RabbitMQConfig;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.ExtractionRequest;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.task.QueryTask;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.task.creation.TableCreationTask;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.task.extraction.DataExtractionTask;
import org.bigdatacenter.healthcarecdcdataextractor.domain.transaction.TrRequestInfo;
import org.bigdatacenter.healthcarecdcdataextractor.resolver.script.ShellScriptResolver;
import org.bigdatacenter.healthcarecdcdataextractor.service.RawDataDBService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.List;

@Component
public class RabbitMQReceiverImpl implements RabbitMQReceiver {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQReceiverImpl.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    private final ShellScriptResolver shellScriptResolver;

    private final RabbitAdmin rabbitAdmin;

    private final RawDataDBService rawDataDBService;

    private final DataIntegrationPlatformAPICaller dataIntegrationPlatformAPICaller;

    @Value("${shellscript.path.home}")
    private String homePath;

    @Autowired
    public RabbitMQReceiverImpl(ShellScriptResolver shellScriptResolver, RabbitAdmin rabbitAdmin, RawDataDBService rawDataDBService, DataIntegrationPlatformAPICaller dataIntegrationPlatformAPICaller) {
        this.shellScriptResolver = shellScriptResolver;
        this.rabbitAdmin = rabbitAdmin;
        this.rawDataDBService = rawDataDBService;
        this.dataIntegrationPlatformAPICaller = dataIntegrationPlatformAPICaller;
    }

    @Override
    public void runReceiver(ExtractionRequest extractionRequest) {
        if (checkExtractionRequestValidity(extractionRequest)) {
            final TrRequestInfo requestInfo = extractionRequest.getRequestInfo();
            final Integer dataSetUID = requestInfo.getDataSetUID();

            try {
                final Long jobStartTime = System.currentTimeMillis();
                dataIntegrationPlatformAPICaller.callUpdateJobStartTime(dataSetUID, jobStartTime);
                dataIntegrationPlatformAPICaller.callUpdateProcessState(dataSetUID, DataIntegrationPlatformAPICaller.PROCESS_STATE_CODE_PROCESSING);

                runQueryTask(extractionRequest);
                runArchiveTask(extractionRequest);

                final Long jobEndTime = System.currentTimeMillis();
                dataIntegrationPlatformAPICaller.callUpdateJobEndTime(dataSetUID, jobEndTime);
                dataIntegrationPlatformAPICaller.callUpdateElapsedTime(dataSetUID, (jobEndTime - jobStartTime));
                dataIntegrationPlatformAPICaller.callUpdateProcessState(dataSetUID, DataIntegrationPlatformAPICaller.PROCESS_STATE_CODE_COMPLETED);
            } catch (Exception e1) {
                try {
                    dataIntegrationPlatformAPICaller.callUpdateProcessState(dataSetUID, DataIntegrationPlatformAPICaller.PROCESS_STATE_CODE_REJECTED);
                    logger.error(String.format("%s - Exception occurs in RabbitMQReceiver : %s", currentThreadName, e1.getMessage()));

                    rabbitAdmin.purgeQueue(RabbitMQConfig.EXTRACTION_REQUEST_QUEUE, true);
                    logger.error(String.format("%s - The extraction request has been purged in queue. (%s)", currentThreadName, extractionRequest));
                    e1.printStackTrace();
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
            }
        } else {
            try {
                rabbitAdmin.purgeQueue(RabbitMQConfig.EXTRACTION_REQUEST_QUEUE, true);
                logger.error(String.format("%s - The extraction request has been purged in queue. (%s)", currentThreadName, extractionRequest));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private Boolean checkExtractionRequestValidity(ExtractionRequest extractionRequest) {
        Boolean isValid = Boolean.TRUE;
        if (extractionRequest == null) {
            logger.error(String.format("%s - Error occurs at RabbitMQReceiver: extraction request is null", currentThreadName));
            isValid = Boolean.FALSE;
        } else if (extractionRequest.getQueryTaskList().size() == 0) {
            logger.error(String.format("%s - Error occurs at RabbitMQReceiver: There are no tasks to process.", currentThreadName));
            isValid = Boolean.FALSE;
        }
        return isValid;
    }

    private void runQueryTask(ExtractionRequest extractionRequest) {
        try {
            final String databaseName = extractionRequest.getDatabaseName();
            final List<QueryTask> queryTaskList = extractionRequest.getQueryTaskList();
            final int queryTaskListSize = queryTaskList.size();

            for (int i = 0; i < queryTaskListSize; i++) {
                final QueryTask queryTask = queryTaskList.get(i);
                final TableCreationTask tableCreationTask = queryTask.getTableCreationTask();
                final DataExtractionTask dataExtractionTask = queryTask.getDataExtractionTask();

                final Long queryBeginTime = System.currentTimeMillis();
                logger.info(String.format("%s - Processing %d/%d query.", currentThreadName, (i + 1), queryTaskListSize));

                if (tableCreationTask != null) {
                    logger.info(String.format("%s - Start table creation at Hive Query: %s", currentThreadName, tableCreationTask.getQuery()));
                    rawDataDBService.createTable(tableCreationTask);
                }

                if (dataExtractionTask != null) {
                    logger.info(String.format("%s - Start data extraction at Hive Query: %s", currentThreadName, dataExtractionTask.getQuery()));
                    rawDataDBService.extractData(dataExtractionTask);

                    //
                    // TODO: Merge Reducer output files in HDFS, download merged file to local file system.
                    //
                    final String dataFileName = dataExtractionTask.getDataFileName();
                    final String hdfsLocation = dataExtractionTask.getHdfsLocation();
                    final String header = dataExtractionTask.getHeader();
                    shellScriptResolver.runReducePartsMerger(hdfsLocation, header, homePath, dataFileName, databaseName);
                }

                final Long queryEndTime = System.currentTimeMillis() - queryBeginTime;
                logger.info(String.format("%s - Finish Hive Query: %s, Elapsed time: %d ms", currentThreadName, queryTask, queryEndTime));
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private void runArchiveTask(ExtractionRequest extractionRequest) {
        try {
            final String databaseName = extractionRequest.getDatabaseName();
            final TrRequestInfo requestInfo = extractionRequest.getRequestInfo();

            //
            // TODO: Archive the extracted data set and finally send the file to FTP server.
            //
            final String archiveFileName = String.format("%s_%s.tar.gz", requestInfo.getUserID(), String.valueOf(new Timestamp(System.currentTimeMillis()).getTime()));
            final String ftpLocation = String.format("/%s/%s", requestInfo.getUserID(), databaseName);

            final long archiveFileBeginTime = System.currentTimeMillis();
            logger.info(String.format("%s - Start archiving the extracted data set: %s", currentThreadName, archiveFileName));
            shellScriptResolver.runArchiveExtractedDataSet(archiveFileName, ftpLocation, homePath, databaseName);
            logger.info(String.format("%s - Finish archiving the extracted data set: %s, Elapsed time: %d ms", currentThreadName, archiveFileName, (System.currentTimeMillis() - archiveFileBeginTime)));

            final String ftpURI = String.format("%s/%s", ftpLocation, archiveFileName);
            dataIntegrationPlatformAPICaller.callCreateFtpInfo(requestInfo.getDataSetUID(), requestInfo.getUserID(), ftpURI);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}