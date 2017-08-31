package org.bigdatacenter.healthcarecdcdataextractor.resolver.extraction;

import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.parameter.ExtractionParameter;
import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.ExtractionRequest;
import org.bigdatacenter.healthcarecdcdataextractor.resolver.extraction.parameter.ExtractionRequestParameterResolver;
import org.bigdatacenter.healthcarecdcdataextractor.resolver.query.select.SelectClauseBuilder;
import org.bigdatacenter.healthcarecdcdataextractor.resolver.query.where.WhereClauseBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Qualifier("extractionRequestResolverForKoges")
public class ExtractionRequestResolverImplForKoges implements ExtractionRequestResolver {
    private static final Logger logger = LoggerFactory.getLogger(ExtractionRequestResolverImplForKoges.class);
    private static final String currentThreadName = Thread.currentThread().getName();

    private final SelectClauseBuilder selectClauseBuilder;

    private final WhereClauseBuilder whereClauseBuilder;

    private final ExtractionRequestParameterResolver extractionRequestParameterResolver;

    @Autowired
    public ExtractionRequestResolverImplForKoges(SelectClauseBuilder selectClauseBuilder,
                                                 WhereClauseBuilder whereClauseBuilder,
                                                 ExtractionRequestParameterResolver extractionRequestParameterResolver) {
        this.selectClauseBuilder = selectClauseBuilder;
        this.whereClauseBuilder = whereClauseBuilder;
        this.extractionRequestParameterResolver = extractionRequestParameterResolver;
    }

    @Override
    public ExtractionRequest buildExtractionRequest(ExtractionParameter extractionParameter) {
        return null;
    }
}
