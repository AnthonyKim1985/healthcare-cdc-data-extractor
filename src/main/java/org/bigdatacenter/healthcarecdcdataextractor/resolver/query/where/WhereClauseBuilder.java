package org.bigdatacenter.healthcarecdcdataextractor.resolver.query.where;

import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.parameter.map.ParameterValue;

import java.util.List;

public interface WhereClauseBuilder {
    String buildClause(List<ParameterValue> parameterValueList);
}