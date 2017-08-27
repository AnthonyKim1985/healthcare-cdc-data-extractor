package org.bigdatacenter.healthcarecdcdataextractor.resolver.extraction.parameter;


import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.parameter.ExtractionRequestParameter;

public interface ExtractionRequestParameterResolver {
    ExtractionRequestParameter buildRequestParameter(ExtractionParameter extractionParameter);
}
