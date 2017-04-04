package com.cairone.odataexample.strategyBuilders;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;

import scala.collection.Iterator;

import com.cairone.odataexample.converters.QueryDslOpsConverter;
import com.cairone.odataexample.entities.QProvinciaEntity;
import com.mysema.query.support.Expressions;
import com.mysema.query.types.Constant;
import com.mysema.query.types.Path;
import com.mysema.query.types.expr.BooleanExpression;
import com.mysema.query.types.expr.BooleanOperation;
import com.sdl.odata.api.ODataException;
import com.sdl.odata.api.ODataSystemException;
import com.sdl.odata.api.parser.CountOption;
import com.sdl.odata.api.parser.ODataUriUtil;
import com.sdl.odata.api.parser.QueryOption;
import com.sdl.odata.api.processor.query.ComparisonCriteria;
import com.sdl.odata.api.processor.query.CompositeCriteria;
import com.sdl.odata.api.processor.query.ContainsMethodCriteria;
import com.sdl.odata.api.processor.query.CountOperation;
import com.sdl.odata.api.processor.query.Criteria;
import com.sdl.odata.api.processor.query.CriteriaFilterOperation;
import com.sdl.odata.api.processor.query.ExpandOperation;
import com.sdl.odata.api.processor.query.LimitOperation;
import com.sdl.odata.api.processor.query.LiteralCriteriaValue;
import com.sdl.odata.api.processor.query.OrderByOperation;
import com.sdl.odata.api.processor.query.OrderByProperty;
import com.sdl.odata.api.processor.query.PropertyCriteriaValue;
import com.sdl.odata.api.processor.query.QueryOperation;
import com.sdl.odata.api.processor.query.SelectByKeyOperation;
import com.sdl.odata.api.processor.query.SelectOperation;
import com.sdl.odata.api.processor.query.SelectPropertiesOperation;
import com.sdl.odata.api.processor.query.SkipOperation;
import com.sdl.odata.api.service.ODataRequestContext;

public class ProvinciasStrategyBuilder {

	private static Logger logger = LoggerFactory.getLogger(ProvinciasStrategyBuilder.class);
	
	private QProvinciaEntity qProvincia = null;
	private BooleanExpression expression = null;
	
	private List<String> propertyNames;
	private List<Sort.Order> orderByList = null;
	private int limit = Integer.MAX_VALUE;
    private int skip = 0;
    private boolean count;
	private boolean includeCount;
	private QueryDslOpsConverter queryDslOpsConverter;

	public BooleanExpression buildCriteria(QueryOperation queryOperation, ODataRequestContext requestContext) throws ODataException {
		
		qProvincia = QProvinciaEntity.provinciaEntity;
		queryDslOpsConverter = new QueryDslOpsConverter();

		buildFromOperation(queryOperation);
        buildFromOptions(ODataUriUtil.getQueryOptions(requestContext.getUri()));
        
        return expression;
	}

	public int getLimit() {
        return limit;
    }

    public int getSkip() {
        return skip;
    }

    public boolean isCount() {
        return count;
    }

    public boolean includeCount() {
        return includeCount;
    }

    public List<String> getPropertyNames() {
        return propertyNames;
    }

    public List<Sort.Order> getOrderByList() {
        return orderByList;
    }

    private void buildFromOperation(QueryOperation operation) throws ODataException {
        if (operation instanceof SelectOperation) {
            buildFromSelect((SelectOperation) operation);
        } else if (operation instanceof SelectByKeyOperation) {
            buildFromSelectByKey((SelectByKeyOperation) operation);
        } else if (operation instanceof CriteriaFilterOperation) {
            buildFromFilter((CriteriaFilterOperation)operation);
        } else if (operation instanceof LimitOperation) {
            buildFromLimit((LimitOperation) operation);
        } else if (operation instanceof CountOperation) {
            buildFromCount((CountOperation) operation);
        } else if (operation instanceof SkipOperation) {
            buildFromSkip((SkipOperation) operation);
        } else if (operation instanceof ExpandOperation) {
        	buildFromExpand((ExpandOperation) operation);
        } else if (operation instanceof OrderByOperation) {
        	buildFromOrderBy((OrderByOperation) operation);
        } else if (operation instanceof SelectPropertiesOperation) {
            buildFromSelectProperties((SelectPropertiesOperation) operation);
        } else {
            throw new ODataSystemException("Unsupported query operation: " + operation);
        }
    }
    
    private void buildFromSelect(SelectOperation selectOperation) {}

    private void buildFromSelectByKey(SelectByKeyOperation selectByKeyOperation) {
        
    	Map<String, Object> keys = selectByKeyOperation.getKeyAsJava();
    	
        Integer provinciaId = Integer.valueOf(keys.get("id").toString());
        Integer paisId = Integer.valueOf(keys.get("paisId").toString());
        
        BooleanExpression exp = qProvincia.id.eq(provinciaId).and(qProvincia.pais.id.eq(paisId));
        this.expression = this.expression == null ? exp : this.expression.and(exp);
    }

    private void buildFromFilter(CriteriaFilterOperation criteriaFilterOperation) throws ODataException {

    	Criteria criteria = criteriaFilterOperation.getCriteria();
    	logger.info("Criteria type: {}", criteria.toString());
        
    	boolean isComposite = criteria instanceof CompositeCriteria;
    	BooleanExpression exp = isComposite ? execCompositeCriteria((CompositeCriteria) criteria) : execSimpleCriteria(criteria);
    	
    	if(exp != null) {
    		this.expression = this.expression == null ? exp : this.expression.and(exp);
    	}
    	
    	buildFromOperation(criteriaFilterOperation.getSource());
    }
    
    private BooleanExpression execCompositeCriteria(Criteria criteria) {
    	
    	if(criteria instanceof CompositeCriteria) {
	    	
    		CompositeCriteria compositeCriteria = (CompositeCriteria) criteria;
    		
	    	Criteria criteriaLeft = compositeCriteria.getLeft();
	    	Criteria criteriaRight = compositeCriteria.getRight();
	    	String operator = compositeCriteria.operator().toString();
	    	
	    	boolean isCompositeLeft = criteriaLeft instanceof CompositeCriteria;
	    	boolean isCompositeRight = criteriaRight instanceof CompositeCriteria;
	    	
	    	BooleanExpression expLeft = isCompositeLeft ? execCompositeCriteria(criteriaLeft) : execSimpleCriteria(criteriaLeft);
	    	BooleanExpression expRight = isCompositeRight ? execCompositeCriteria(criteriaRight) : execSimpleCriteria(criteriaRight);
	    	BooleanExpression exp = BooleanOperation.create(queryDslOpsConverter.convertToAnotherAttribute(operator), expLeft, expRight);
	    	
	    	return exp;
    	}
    	
    	return null;
    }
    
    private BooleanExpression execSimpleCriteria(Criteria criteria) {

    	if(criteria instanceof ComparisonCriteria) {
    		ComparisonCriteria comparisonCriteria = (ComparisonCriteria) criteria;
            return execComparisonCriteria(comparisonCriteria);
        }

    	if(criteria instanceof ContainsMethodCriteria) {
    		ContainsMethodCriteria containsMethodCriteria = (ContainsMethodCriteria) criteria;
    		return execContainsMethodCriteria(containsMethodCriteria);
    	}
    	
    	return null;
    }

    private BooleanExpression execContainsMethodCriteria(ContainsMethodCriteria containsMethodCriteria) {

		PropertyCriteriaValue propertyCriteriaValue = (PropertyCriteriaValue) containsMethodCriteria.getProperty();
        LiteralCriteriaValue literalCriteriaValue = (LiteralCriteriaValue) containsMethodCriteria.getStringLiteral();
        
        String field = propertyCriteriaValue.getPropertyName().trim().toUpperCase();
        Object value = literalCriteriaValue.getValue();
        
        switch(field)
        {
        case "NOMBRE":
        {
        	String nombre = value.toString();
        	return qProvincia.nombre.contains(nombre);
        }
        case "PAIS.NOMBRE":
        {
        	String nombre = value.toString();
        	return qProvincia.pais.nombre.contains(nombre);
        }
        }
        
        return null;
    }
    
    private BooleanExpression execComparisonCriteria(ComparisonCriteria comparisonCriteria) {

        PropertyCriteriaValue propertyCriteriaValue = getPropertyCriteriaValue(comparisonCriteria);
        LiteralCriteriaValue literalCriteriaValue = getLiteralCriteriaValue(comparisonCriteria);
        
        String field = propertyCriteriaValue.getPropertyName().trim().toUpperCase();
        Object value = literalCriteriaValue.getValue();
        String operator = comparisonCriteria.operator().toString();
        
        switch(field)
        {
        case "ID":
        {
        	Integer idValue = Integer.valueOf(value.toString());

        	Path<Integer> provinciaIdPath = Expressions.path(Integer.class, qProvincia, "id");
        	Constant<Integer> constant = (Constant<Integer>) Expressions.constant(idValue);

        	return Expressions.predicate(queryDslOpsConverter.convertToAnotherAttribute(operator), provinciaIdPath, constant);
        }
        case "NOMBRE":
        {
        	String descripcionValue = value.toString();
        	
        	Path<String> provinciaNombrePath = Expressions.path(String.class, qProvincia, "nombre");
        	Constant<String> constant = (Constant<String>) Expressions.constant(descripcionValue);
        	
        	return Expressions.predicate(queryDslOpsConverter.convertToAnotherAttribute(operator), provinciaNombrePath, constant);
        }
        case "PAISID":
        case "PAIS.ID":
        {
        	Integer paisIdValue = Integer.valueOf(value.toString());

        	Path<Integer> paisIdPath = Expressions.path(Integer.class, qProvincia.pais, "id");
        	Constant<Integer> constant = (Constant<Integer>) Expressions.constant(paisIdValue);

        	return Expressions.predicate(queryDslOpsConverter.convertToAnotherAttribute(operator), paisIdPath, constant);
        }
        case "PAIS.NOMBRE":
        {
        	String paisNombreValue = value.toString();

        	Path<String> paisNombrePath = Expressions.path(String.class, qProvincia.pais, "nombre");
        	Constant<String> constant = (Constant<String>) Expressions.constant(paisNombreValue);
        	
        	return Expressions.predicate(queryDslOpsConverter.convertToAnotherAttribute(operator), paisNombrePath, constant);
        }
        case "PAIS.PREFIJO":
        {
        	Integer paisPrefijoValue = Integer.valueOf(value.toString());

        	Path<Integer> paisPrefijoPath = Expressions.path(Integer.class, qProvincia.pais, "prefijo");
        	Constant<Integer> constant = (Constant<Integer>) Expressions.constant(paisPrefijoValue);
        	
        	return Expressions.predicate(queryDslOpsConverter.convertToAnotherAttribute(operator), paisPrefijoPath, constant);
        }
        }
        
        return null;
    }

    private PropertyCriteriaValue getPropertyCriteriaValue(ComparisonCriteria comparisonCriteria) {
    	
    	if(comparisonCriteria.getLeft() instanceof PropertyCriteriaValue) {
    		return (PropertyCriteriaValue) comparisonCriteria.getLeft();
    	}
    	
    	if(comparisonCriteria.getRight() instanceof PropertyCriteriaValue) {
    		return (PropertyCriteriaValue) comparisonCriteria.getRight();
    	}
    	
    	return null;
    }

    private LiteralCriteriaValue getLiteralCriteriaValue(ComparisonCriteria comparisonCriteria) {
    	
    	if(comparisonCriteria.getLeft() instanceof LiteralCriteriaValue) {
    		return (LiteralCriteriaValue) comparisonCriteria.getLeft();
    	}
    	
    	if(comparisonCriteria.getRight() instanceof LiteralCriteriaValue) {
    		return (LiteralCriteriaValue) comparisonCriteria.getRight();
    	}
    	
    	return null;
    }

    private void buildFromLimit(LimitOperation operation) throws ODataException {
        this.limit = operation.getCount();
        buildFromOperation(operation.getSource());
    }

    private void buildFromCount(CountOperation operation) throws ODataException {
        this.count = operation.trueFalse();
        buildFromOperation(operation.getSource());
    }

    private void buildFromSkip(SkipOperation operation) throws ODataException {
        this.skip = operation.getCount();
        buildFromOperation(operation.getSource());
    }
    
    private void buildFromExpand(ExpandOperation expandOperation) throws ODataException {
    	buildFromOperation(expandOperation.getSource());
    }

    private void buildFromOrderBy(OrderByOperation orderByOperation) throws ODataException {
    	
    	List<OrderByProperty> orderByProperties = orderByOperation.getOrderByPropertiesAsJava();
    	
    	if(orderByProperties != null && orderByProperties.size() > 0) {
    		
    		orderByList = new ArrayList<Sort.Order>();
    		
    		for(OrderByProperty orderByProperty : orderByProperties) {
    			
    			Direction direction = orderByProperty.getDirection().toString().equals("ASC") ? Direction.ASC : Direction.DESC;
    			String property = orderByProperty.getPropertyName();
    			
    			orderByList.add(new Sort.Order(direction, property));
    		}
    	}
    	
    	buildFromOperation(orderByOperation.getSource());
    }

    private void buildFromSelectProperties(SelectPropertiesOperation operation) throws ODataException {
        this.propertyNames = operation.getPropertyNamesAsJava();
        buildFromOperation(operation.getSource());
    }

    private void buildFromOptions(scala.collection.immutable.List<QueryOption> queryOptions) {
        Iterator<QueryOption> optIt = queryOptions.iterator();
        while (optIt.hasNext()) {
            QueryOption opt = optIt.next();
            if (opt instanceof CountOption && ((CountOption) opt).value()) {
                includeCount = true;
                break;
            }
        }
    }
}
