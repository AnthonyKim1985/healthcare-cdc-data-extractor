package org.bigdatacenter.healthcarecdcdataextractor.resolver.extraction;

import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.ExtractionRequest;

public interface ExtractionRequestResolver {
    ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter);
}
