package org.bigdatacenter.healthcarecdcdataextractor.resolver.query.join;

import org.bigdatacenter.healthcarecdcdataextractor.domain.extraction.request.query.JoinParameter;

import java.util.List;

public interface JoinClauseBuilder {
    String buildClause(List<JoinParameter> joinParameterList);

    String buildClause(JoinParameter sourceJoinParameter, JoinParameter targetJoinParameter, Boolean isKogesDataSet);
}
