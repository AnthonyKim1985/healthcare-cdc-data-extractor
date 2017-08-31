package org.bigdatacenter.healthcarecdcdataextractor.api;

import com.google.gson.Gson;
import org.bigdatacenter.healthcarecdcdataextractor.api.caller.DataIntegrationPlatformAPICaller;
import org.bigdatacenter.healthcarecdcdataextractor.config.RabbitMQConfig;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.ExtractionRequest;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.response.ExtractionResponse;
import org.bigdatacenter.healthcarecdcdataextractor.exception.RESTException;
import org.bigdatacenter.healthcarecdcdataextractor.resolver.extraction.ExtractionRequestResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.text.SimpleDateFormat;
import java.util.Date;

@RestController
@RequestMapping("/extraction/api")
public class DataExtractionController {
    private static final Logger logger = LoggerFactory.getLogger(DataExtractionController.class);
    private static final String currentThreadName = Thread.currentThread().getName();
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final int CHS = 6;
    private static final int KNHANES = 7;
    private static final int KOGES = 9;

    private final RabbitTemplate rabbitTemplate;

    private final DataIntegrationPlatformAPICaller dataIntegrationPlatformAPICaller;

    private final ExtractionRequestResolver extractionRequestResolverForKoges;

    private final ExtractionRequestResolver extractionRequestResolverForGeneral;

    @Autowired
    public DataExtractionController(RabbitTemplate rabbitTemplate,
                                    DataIntegrationPlatformAPICaller dataIntegrationPlatformAPICaller,
                                    @Qualifier("extractionRequestResolverForKoges") ExtractionRequestResolver extractionRequestResolverForKoges,
                                    @Qualifier("extractionRequestResolverForGeneral") ExtractionRequestResolver extractionRequestResolverForGeneral) {
        this.rabbitTemplate = rabbitTemplate;
        this.dataIntegrationPlatformAPICaller = dataIntegrationPlatformAPICaller;
        this.extractionRequestResolverForKoges = extractionRequestResolverForKoges;
        this.extractionRequestResolverForGeneral = extractionRequestResolverForGeneral;
    }

    @ResponseBody
    @ResponseStatus(HttpStatus.OK)
    @RequestMapping(value = "dataExtraction", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ExtractionResponse dataExtraction(@RequestBody ExtractionParameter extractionParameter, HttpServletResponse httpServletResponse) {
        final ExtractionRequest extractionRequest;
        final ExtractionResponse extractionResponse;
        final Integer dataSetUID = extractionParameter.getRequestInfo().getDataSetUID();
        try {
            logger.info(String.format("%s - extractionParameter: %s", currentThreadName, extractionParameter));
            extractionRequest = resolveExtractionRequest(extractionParameter);

            synchronized (this) {
                rabbitTemplate.convertAndSend(RabbitMQConfig.EXTRACTION_REQUEST_QUEUE, extractionRequest);
            }

            final String jobAcceptedTime = dateFormat.format(new Date(System.currentTimeMillis()));
            final String jsonForExtractionRequest = new Gson().toJson(extractionRequest, ExtractionRequest.class);
            extractionResponse = new ExtractionResponse(jobAcceptedTime, jsonForExtractionRequest);
        } catch (Exception e) {
            e.printStackTrace();
            dataIntegrationPlatformAPICaller.callUpdateProcessState(dataSetUID, DataIntegrationPlatformAPICaller.PROCESS_STATE_CODE_REJECTED);
            throw new RESTException(String.format("Bad request (%s)", e.getMessage()), httpServletResponse);
        }

        return extractionResponse;
    }

    private ExtractionRequest resolveExtractionRequest(ExtractionParameter extractionParameter) {
        final Integer dataSetID = extractionParameter.getRequestInfo().getDatasetID();
        final ExtractionRequest extractionRequest;

        try {
            switch (dataSetID) {
                case CHS:
                case KNHANES:
                    extractionRequest = extractionRequestResolverForGeneral.buildExtractionRequest(extractionParameter);
                    break;
                case KOGES:
                    extractionRequest = extractionRequestResolverForKoges.buildExtractionRequest(extractionParameter);
                    break;
                default:
                    throw new RuntimeException(String.format("Bad data set id: %d", dataSetID));
            }
            return extractionRequest;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}