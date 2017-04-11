package com.cairone.odataexample.odataqueryoptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cairone.odataexample.utils.JPAMetadataUtil;
import com.sdl.odata.api.ODataException;
import com.sdl.odata.api.ODataNotImplementedException;
import com.sdl.odata.api.edm.model.EntityType;
import com.sdl.odata.api.processor.query.ArithmeticCriteriaValue;
import com.sdl.odata.api.processor.query.ComparisonCriteria;
import com.sdl.odata.api.processor.query.CompositeCriteria;
import com.sdl.odata.api.processor.query.ContainsMethodCriteria;
import com.sdl.odata.api.processor.query.Criteria;
import com.sdl.odata.api.processor.query.CriteriaValue;
import com.sdl.odata.api.processor.query.EndsWithMethodCriteria;
import com.sdl.odata.api.processor.query.LiteralCriteriaValue;
import com.sdl.odata.api.processor.query.ModOperator$;
import com.sdl.odata.api.processor.query.PropertyCriteriaValue;
import com.sdl.odata.api.processor.query.StartsWithMethodCriteria;

public class JPQLWhereStrategyBuilder {
	
    private static final Logger LOG = LoggerFactory.getLogger(JPQLWhereStrategyBuilder.class);

    private static final String PREFIX_PARAM = "value";

    private final EntityType targetEntityType;

    private final JPQLQueryBuilder jpaQueryBuilder;

    private int paramCount = 0;

    public JPQLWhereStrategyBuilder(EntityType targetEntityType, JPQLQueryBuilder jpaQueryBuilder) {
        this.targetEntityType = targetEntityType;
        this.jpaQueryBuilder = jpaQueryBuilder;
    }

    public void build(Criteria criteria) throws ODataException {
        LOG.debug("where clause is going to build for {}", criteria);
        StringBuilder builder = new StringBuilder();
        buildFromCriteria(criteria, builder);
        jpaQueryBuilder.setWhereClause(builder.toString());
        LOG.debug("where clause built for {}", criteria);
    }

    private void buildFromCriteria(Criteria criteria, StringBuilder builder) throws ODataException {
        if (criteria instanceof CompositeCriteria) {
            buildFromCompositeCriteria((CompositeCriteria) criteria, builder);
        } else if (criteria instanceof ComparisonCriteria) {
            buildFromComparisonCriteria((ComparisonCriteria) criteria, builder);
        } else if (criteria instanceof ContainsMethodCriteria) {
        	buildFromContainsMethodCriteria((ContainsMethodCriteria) criteria, builder);
        } else if (criteria instanceof EndsWithMethodCriteria) {
        	buildFromEndsWithMethodCriteria((EndsWithMethodCriteria) criteria, builder);
        } else if (criteria instanceof StartsWithMethodCriteria) {
        	buildFromStartsWithMethodCriteria((StartsWithMethodCriteria) criteria, builder);
        } else {
            throw new ODataNotImplementedException("Unsupported criteria type: " + criteria);
        }
    }
    
    private void buildFromStartsWithMethodCriteria(StartsWithMethodCriteria criteria, StringBuilder builder) throws ODataException {

    	PropertyCriteriaValue propertyCriteriaValue = (PropertyCriteriaValue) criteria.getProperty();
        LiteralCriteriaValue literalCriteriaValue = (LiteralCriteriaValue) criteria.getStringLiteral();
        
        String field = propertyCriteriaValue.getPropertyName();
        String paramName = PREFIX_PARAM + (++paramCount);
        
        builder
        	.append(field)
        	.append(" LIKE :")
        	.append(paramName);
    	
        jpaQueryBuilder.addParam(paramName, literalCriteriaValue.value() + "%");
    }

    private void buildFromEndsWithMethodCriteria(EndsWithMethodCriteria criteria, StringBuilder builder) throws ODataException {

    	PropertyCriteriaValue propertyCriteriaValue = (PropertyCriteriaValue) criteria.getProperty();
        LiteralCriteriaValue literalCriteriaValue = (LiteralCriteriaValue) criteria.getStringLiteral();
        
        String field = propertyCriteriaValue.getPropertyName();
        String paramName = PREFIX_PARAM + (++paramCount);
        
        builder
        	.append(field)
        	.append(" LIKE :")
        	.append(paramName);
    	
        jpaQueryBuilder.addParam(paramName, "%" + literalCriteriaValue.value());
    }
    
    private void buildFromContainsMethodCriteria(ContainsMethodCriteria criteria, StringBuilder builder) throws ODataException {
    	
    	PropertyCriteriaValue propertyCriteriaValue = (PropertyCriteriaValue) criteria.getProperty();
        LiteralCriteriaValue literalCriteriaValue = (LiteralCriteriaValue) criteria.getStringLiteral();
        
        String field = propertyCriteriaValue.getPropertyName();
        String paramName = PREFIX_PARAM + (++paramCount);
        
        builder
        	.append(field)
        	.append(" LIKE :")
        	.append(paramName);
    	
        jpaQueryBuilder.addParam(paramName, "%" + literalCriteriaValue.value() + "%");
    }

    private void buildFromComparisonCriteria(ComparisonCriteria criteria, StringBuilder builder) throws ODataException {
        builder.append("(");
        buildFromCriteriaValue(criteria.left(), builder);
        builder.append(' ').append(criteria.operator().toString()).append(' ');
        buildFromCriteriaValue(criteria.right(), builder);
        builder.append(")");
    }

    private void buildFromCriteriaValue(CriteriaValue value, StringBuilder builder) throws ODataException {
        if (value instanceof LiteralCriteriaValue) {
            buildFromLiteralCriteriaValue((LiteralCriteriaValue) value, builder);
        } else if (value instanceof ArithmeticCriteriaValue) {
            buildFromArithmeticCriteriaValue((ArithmeticCriteriaValue) value, builder);
        } else if (value instanceof PropertyCriteriaValue) {
            buildFromPropertyCriteriaValue((PropertyCriteriaValue) value, builder);
        } else {
            throw new ODataNotImplementedException("Unsupported criteria value type: " + value);
        }
    }

    private void buildFromLiteralCriteriaValue(LiteralCriteriaValue value, StringBuilder builder) {
        String paramName = PREFIX_PARAM + (++paramCount);
        builder.append(':').append(paramName);
        jpaQueryBuilder.addParam(paramName, value.value());
    }

    private void buildFromArithmeticCriteriaValue(ArithmeticCriteriaValue value, StringBuilder builder) throws ODataException {
        
        if (value.operator() == ModOperator$.MODULE$) {
            builder.append("MOD(");
            buildFromCriteriaValue(value.left(), builder);
            builder.append(", ");
            buildFromCriteriaValue(value.right(), builder);
            builder.append(")");
        } else {
            buildFromCriteriaValue(value.left(), builder);
            builder.append(' ').append(value.operator().toString()).append(' ');
            buildFromCriteriaValue(value.right(), builder);
        }
    }

    private void buildFromPropertyCriteriaValue(PropertyCriteriaValue value, StringBuilder builder) {
        builder.append(jpaQueryBuilder.getFromAlias());
        builder.append(".");
        try {
        	builder.append(JPAMetadataUtil.getJPAPropertyName(targetEntityType, value.propertyName()));
		} catch (Exception e) {
			builder.append(value.propertyName());
		}
    }

    private void buildFromCompositeCriteria(CompositeCriteria criteria, StringBuilder builder) throws ODataException {
        builder.append("(");
        buildFromCriteria(criteria.left(), builder);
        builder.append(' ').append(criteria.operator().toString()).append(' ');
        buildFromCriteria(criteria.right(), builder);
        builder.append(")");
    }

    public int getParamCount() {
        return paramCount;
    }

    public JPQLWhereStrategyBuilder setParamCount(int paramCount) {
        this.paramCount = paramCount;
        return this;
    }
}
