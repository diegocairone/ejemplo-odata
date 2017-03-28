package com.cairone.odataexample.strategyBuilders;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;

import scala.collection.Iterator;

import com.cairone.odataexample.entities.PersonaPKEntity;
import com.cairone.odataexample.entities.QPersonaEntity;
import com.mysema.query.types.expr.BooleanExpression;
import com.sdl.odata.api.ODataException;
import com.sdl.odata.api.ODataSystemException;
import com.sdl.odata.api.parser.CountOption;
import com.sdl.odata.api.parser.ODataUriUtil;
import com.sdl.odata.api.parser.QueryOption;
import com.sdl.odata.api.processor.query.ComparisonCriteria;
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

public class PersonasStrategyBuilder {

	private QPersonaEntity qPersona = null;
	private BooleanExpression expression = null;
	
	private List<String> propertyNames;
	private List<Sort.Order> orderByList = null;
	private int limit = Integer.MAX_VALUE;
    private int skip = 0;
    private boolean count;
	private boolean includeCount;
	
	public BooleanExpression buildCriteria(QueryOperation queryOperation, ODataRequestContext requestContext) throws ODataException {
		
		qPersona = QPersonaEntity.personaEntity;

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
    	
        Integer tipoDocumentoId = Integer.valueOf(keys.get("tipoDocumentoId").toString());
        String numeroDocumento = keys.get("numeroDocumento").toString();
        
        BooleanExpression exp = qPersona.pk.eq(new PersonaPKEntity(tipoDocumentoId, numeroDocumento));
        this.expression = this.expression == null ? exp : this.expression.and(exp);
    }
    
    private void buildFromFilter(CriteriaFilterOperation criteriaFilterOperation) {
    	
    	Criteria criteria = criteriaFilterOperation.getCriteria();
        
    	if(criteria instanceof ComparisonCriteria) {
    		ComparisonCriteria comparisonCriteria = (ComparisonCriteria) criteria;
            execComparisonCriteria(comparisonCriteria);
        }

    	if(criteria instanceof ContainsMethodCriteria) {
    		
    		ContainsMethodCriteria containsMethodCriteria = (ContainsMethodCriteria) criteria;
    		execContainsMethodCriteria(containsMethodCriteria);
    	}
    	
    }

    private void execComparisonCriteria(ComparisonCriteria comparisonCriteria) {

        if(comparisonCriteria.getLeft() instanceof PropertyCriteriaValue && comparisonCriteria.getRight() instanceof LiteralCriteriaValue) {

            PropertyCriteriaValue propertyCriteriaValue = (PropertyCriteriaValue) comparisonCriteria.getLeft();
            LiteralCriteriaValue literalCriteriaValue = (LiteralCriteriaValue) comparisonCriteria.getRight();
            
            String field = propertyCriteriaValue.getPropertyName().trim().toUpperCase();
            Object value = literalCriteriaValue.getValue();
            
            switch(field)
            {
            case "APELLIDOS":
            {
            	String apellidosValue = (String) value;
            	BooleanExpression exp = qPersona.apellidos.eq(apellidosValue);
                this.expression = this.expression == null ? exp : this.expression.and(exp);
            	break;
            }
            case "NOMBRES":
            {
            	String nombresValue = (String) value;
            	BooleanExpression exp = qPersona.nombres.eq(nombresValue);
                this.expression = this.expression == null ? exp : this.expression.and(exp);
            	break;
            }
            }
        }
    }
    
    private void execContainsMethodCriteria(ContainsMethodCriteria containsMethodCriteria) {

		PropertyCriteriaValue propertyCriteriaValue = (PropertyCriteriaValue) containsMethodCriteria.getProperty();
        LiteralCriteriaValue literalCriteriaValue = (LiteralCriteriaValue) containsMethodCriteria.getStringLiteral();
        
        String field = propertyCriteriaValue.getPropertyName().trim().toUpperCase();
        Object value = literalCriteriaValue.getValue();
        
        switch(field)
        {
        case "APELLIDOS":
        {
        	String apellidosValue = (String) value;
        	BooleanExpression exp = qPersona.apellidos.contains(apellidosValue);
            this.expression = this.expression == null ? exp : this.expression.and(exp);
        	break;
        }
        case "NOMBRES":
        {
        	String nombresValue = (String) value;
        	BooleanExpression exp = qPersona.nombres.contains(nombresValue);
            this.expression = this.expression == null ? exp : this.expression.and(exp);
        	break;
        }
        }
    }

    private void buildFromLimit(LimitOperation operation) throws ODataException {
        this.limit = operation.getCount();
        buildFromOperation(operation.getSource());
    }

    private void buildFromCount(CountOperation operation) throws ODataException {
        this.count = true;
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
