package com.cairone.odataexample.strategyBuilders;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;

import scala.collection.Iterator;

import com.cairone.odataexample.annotations.ODataJPAEntity;
import com.cairone.odataexample.converters.QueryDslOpsConverter;
import com.cairone.odataexample.entities.PaisEntity;
import com.mysema.query.support.Expressions;
import com.mysema.query.types.Constant;
import com.mysema.query.types.Ops;
import com.mysema.query.types.Path;
import com.mysema.query.types.PathMetadataFactory;
import com.mysema.query.types.expr.BooleanExpression;
import com.mysema.query.types.path.EntityPathBase;
import com.mysema.query.types.path.PathInits;
import com.sdl.odata.api.ODataException;
import com.sdl.odata.api.ODataSystemException;
import com.sdl.odata.api.edm.model.EntityDataModel;
import com.sdl.odata.api.edm.model.EntitySet;
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

public class ProvinciasStrategyBuilder {

	private EntityPathBase<?> entityPathBase = null;
	private BooleanExpression expression = null;
	
	private List<String> propertyNames;
	private List<Sort.Order> orderByList = null;
	private int limit = Integer.MAX_VALUE;
    private int skip = 0;
    private boolean count;
	private boolean includeCount;
	
	private QueryDslOpsConverter queryDslOpsConverter;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public BooleanExpression buildCriteria(QueryOperation queryOperation, ODataRequestContext requestContext) throws ODataException {
		
		EntityDataModel entityDataModel = requestContext.getEntityDataModel();
		EntitySet entitySet = entityDataModel.getEntityContainer().getEntitySet(queryOperation.entitySetName());
        Class<?> odataEntityType = entityDataModel.getType(entitySet.getTypeName()).getJavaType();

        ODataJPAEntity odataJPAEntityAnnotation = odataEntityType.getAnnotation(ODataJPAEntity.class);
        Class<?> jpaEntity = null;
        
        try {
			jpaEntity = Class.forName(odataJPAEntityAnnotation.value());
		} catch (ClassNotFoundException e) {
			throw new ODataSystemException(e.getMessage());
		}
		
		//entityPathBase = new EntityPathBase<ProvinciaEntity>(ProvinciaEntity.class, PathMetadataFactory.forVariable("provinciaEntity"), PathInits.DIRECT2);
        entityPathBase = new EntityPathBase(jpaEntity, PathMetadataFactory.forVariable("provinciaEntity"), PathInits.DIRECT2);
        
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
        
        Path<Integer> pathProvinciaId = Expressions.path(Integer.class, entityPathBase, "id");
        Path<PaisEntity> pathPais = Expressions.path(PaisEntity.class, entityPathBase, "pais");
        Path<Integer> pathPaisId = Expressions.path(Integer.class, pathPais, "id");
        
    	Constant<Integer> cProvinciaId = (Constant<Integer>) Expressions.constant(provinciaId);
    	Constant<Integer> cPaisId = (Constant<Integer>) Expressions.constant(paisId);
    	
    	BooleanExpression exp = Expressions.predicate(Ops.EQ, pathProvinciaId, cProvinciaId).and(Expressions.predicate(Ops.EQ, pathPaisId, cPaisId));
        
        this.expression = this.expression == null ? exp : this.expression.and(exp);
    }

    private void buildFromFilter(CriteriaFilterOperation criteriaFilterOperation) throws ODataException {
    	
    	Criteria criteria = criteriaFilterOperation.getCriteria();
        
    	if(criteria instanceof ComparisonCriteria) {
    		
            ComparisonCriteria comparisonCriteria = (ComparisonCriteria) criteria;
            execComparisonCriteria(comparisonCriteria);
        }

    	if(criteria instanceof ContainsMethodCriteria) {
    		
    		ContainsMethodCriteria containsMethodCriteria = (ContainsMethodCriteria) criteria;
    		execContainsMethodCriteria(containsMethodCriteria);
    	}
    	
    	buildFromOperation(criteriaFilterOperation.getSource());
    }

    private void execContainsMethodCriteria(ContainsMethodCriteria containsMethodCriteria) {

		PropertyCriteriaValue propertyCriteriaValue = (PropertyCriteriaValue) containsMethodCriteria.getProperty();
        LiteralCriteriaValue literalCriteriaValue = (LiteralCriteriaValue) containsMethodCriteria.getStringLiteral();
        
        String field = propertyCriteriaValue.getPropertyName().trim().toUpperCase();
        Object value = literalCriteriaValue.getValue();
        
        switch(field)
        {
        case "NOMBRE":
        {
//        	String nombre = value.toString();
//        	BooleanExpression exp = qProvincia.nombre.contains(nombre);
//            this.expression = this.expression == null ? exp : this.expression.and(exp);
//        	break;
        }
        }
    }

    private void execComparisonCriteria(ComparisonCriteria comparisonCriteria) {

        if(comparisonCriteria.getLeft() instanceof PropertyCriteriaValue && comparisonCriteria.getRight() instanceof LiteralCriteriaValue) {

            PropertyCriteriaValue propertyCriteriaValue = (PropertyCriteriaValue) comparisonCriteria.getLeft();
            LiteralCriteriaValue literalCriteriaValue = (LiteralCriteriaValue) comparisonCriteria.getRight();
            
            String field = propertyCriteriaValue.getPropertyName().trim().toUpperCase();
            Object value = literalCriteriaValue.getValue();
            String operator = comparisonCriteria.operator().toString();
            
            switch(field)
            {
            case "ID":
            {
//            	Integer idValue = (Integer) value;
//            	BooleanExpression exp = qProvincia.id.eq(idValue);
//            	this.expression = this.expression == null ? exp : this.expression.and(exp);
            	break;
            }
            case "NOMBRE":
            {
            	String descripcionValue = (String) value;
            	
            	//Path<String> provinciaNombre = Expressions.path(String.class, qProvincia, "nombre");
            	Path<String> provinciaNombre = Expressions.path(String.class, entityPathBase, "nombre");            	
            	Constant<String> constant = (Constant<String>) Expressions.constant(descripcionValue);
            	BooleanExpression exp = Expressions.predicate(queryDslOpsConverter.convertToAnotherAttribute(operator), provinciaNombre, constant);
            	
//            	String descripcionValue = (String) value;
//            	BooleanExpression exp = qProvincia.nombre.eq(descripcionValue);
            	this.expression = this.expression == null ? exp : this.expression.and(exp);
            	break;
            }
            case "PAISID":
            case "PAIS.ID":
            {
//            	Integer paisIdValue = Integer.valueOf(value.toString());
//            	BooleanExpression exp = qProvincia.pais.id.eq(paisIdValue);
//                this.expression = this.expression == null ? exp : this.expression.and(exp);
            	break;
            }
            case "PAIS.NOMBRE":
            {
//            	String paisNombreValue = (String) value;
//            	BooleanExpression exp = qProvincia.pais.nombre.eq(paisNombreValue);
//                this.expression = this.expression == null ? exp : this.expression.and(exp);
            	break;
            }
            case "PAIS.PREFIJO":
            {
//            	Integer paisPrefijoValue = Integer.valueOf(value.toString());
//            	BooleanExpression exp = qProvincia.pais.prefijo.eq(paisPrefijoValue);
//                this.expression = this.expression == null ? exp : this.expression.and(exp);
            	break;
            }
            }
        }
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
