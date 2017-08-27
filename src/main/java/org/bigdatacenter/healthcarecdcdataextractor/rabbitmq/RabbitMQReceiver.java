package org.bigdatacenter.healthcarecdcdataextractor.rabbitmq;

import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.ExtractionRequest;

public interface RabbitMQReceiver {
    void runReceiver(ExtractionRequest extractionRequest);
}
