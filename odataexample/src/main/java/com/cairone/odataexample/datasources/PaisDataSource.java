package com.cairone.odataexample.datasources;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.MessageSource;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

import scala.Option;

import com.cairone.odataexample.dtos.PaisFrmDto;
import com.cairone.odataexample.dtos.validators.PaisFrmDtoValidator;
import com.cairone.odataexample.edm.resources.PaisEdm;
import com.cairone.odataexample.entities.PaisEntity;
import com.cairone.odataexample.services.PaisService;
import com.cairone.odataexample.strategyBuilders.PaisesStrategyBuilder;
import com.cairone.odataexample.utils.GenJsonOdataSelect;
import com.cairone.odataexample.utils.SQLExceptionParser;
import com.cairone.odataexample.utils.ValidatorUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.mysema.query.types.expr.BooleanExpression;
import com.sdl.odata.api.ODataException;
import com.sdl.odata.api.ODataSystemException;
import com.sdl.odata.api.edm.model.EntityDataModel;
import com.sdl.odata.api.parser.ODataUri;
import com.sdl.odata.api.parser.ODataUriUtil;
import com.sdl.odata.api.parser.TargetType;
import com.sdl.odata.api.processor.datasource.DataSource;
import com.sdl.odata.api.processor.datasource.DataSourceProvider;
import com.sdl.odata.api.processor.datasource.ODataDataSourceException;
import com.sdl.odata.api.processor.datasource.TransactionalDataSource;
import com.sdl.odata.api.processor.link.ODataLink;
import com.sdl.odata.api.processor.query.QueryOperation;
import com.sdl.odata.api.processor.query.QueryResult;
import com.sdl.odata.api.processor.query.strategy.QueryOperationStrategy;
import com.sdl.odata.api.service.ODataRequestContext;

@Component
public class PaisDataSource implements DataSourceProvider, DataSource  {

	@Autowired private PaisService paisService = null;
	@Autowired private PaisFrmDtoValidator paisFrmDtoValidator = null;
	
	@Autowired
	private MessageSource messageSource = null;
	
	@Override
	public Object create(ODataUri uri, Object entity, EntityDataModel entityDataModel) throws ODataException {
		
		if(entity instanceof PaisEdm) {
			
			PaisEdm paisEdm = (PaisEdm) entity;
    		PaisFrmDto paisFrmDto = new PaisFrmDto(paisEdm);

			ValidatorUtil.validate(paisFrmDtoValidator, messageSource, paisFrmDto);

			try {
				PaisEntity paisEntity = paisService.nuevo(paisFrmDto);
				return new PaisEdm(paisEntity);
			} catch (Exception e) {
				String message = SQLExceptionParser.parse(e);
				throw new ODataDataSourceException(message);
			}
		}
		
		throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD PAIS");
	}

	@Override
	public Object update(ODataUri uri, Object entity, EntityDataModel entityDataModel) throws ODataException {

    	if(entity instanceof PaisEdm) {
    		
    		Map<String, Object> oDataUriKeyValues = ODataUriUtil.asJavaMap(ODataUriUtil.getEntityKeyMap(uri, entityDataModel));
    		
    		PaisEdm pais = (PaisEdm) entity;
    		PaisFrmDto paisFrmDto = new PaisFrmDto(pais);

			ValidatorUtil.validate(paisFrmDtoValidator, messageSource, paisFrmDto);

        	Integer paisID = Integer.valueOf(oDataUriKeyValues.get("id").toString());
        	paisFrmDto.setId(paisID);
        	
			try {
				PaisEntity paisEntity = paisService.nuevo(paisFrmDto);
				return new PaisEdm(paisEntity);
			} catch (Exception e) {
				String message = SQLExceptionParser.parse(e);
				throw new ODataDataSourceException(message);
			}
    	}
    	
    	throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD PAIS");
	}

	@Override
	public void delete(ODataUri uri, EntityDataModel entityDataModel) throws ODataException {
				
		Option<Object> entity = ODataUriUtil.extractEntityWithKeys(uri, entityDataModel);
    	
    	if(entity.isDefined()) {
    		
    		PaisEdm paisEdm = (PaisEdm) entity.get();
    		Integer paisID = paisEdm.getId();
    		
    		try {
    			paisService.borrar(paisID);
				return;
			} catch (Exception e) {
				String message = SQLExceptionParser.parse(e);
				throw new ODataDataSourceException(message);
			}
        }
    	
    	throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD PAIS");
	}

	@Override
	public void createLink(ODataUri uri, ODataLink link, EntityDataModel entityDataModel) throws ODataException {
	}

	@Override
	public void deleteLink(ODataUri uri, ODataLink link, EntityDataModel entityDataModel) throws ODataException {
	}

	@Override
	public TransactionalDataSource startTransaction() {
		throw new ODataSystemException("No support for transactions");
	}
	
	@Override
	public boolean isSuitableFor(ODataRequestContext requestContext, String entityType) throws ODataDataSourceException {
		return requestContext.getEntityDataModel().getType(entityType).getJavaType().equals(PaisEdm.class);
	}

	@Override
	public DataSource getDataSource(ODataRequestContext requestContext) {
		return this;
	}

	@Override
	public QueryOperationStrategy getStrategy(ODataRequestContext requestContext, QueryOperation operation, TargetType expectedODataEntityType) throws ODataException {

		PaisesStrategyBuilder builder = new PaisesStrategyBuilder();
		BooleanExpression expression = builder.buildCriteria(operation, requestContext);
		List<Sort.Order> orderByList = builder.getOrderByList();
		
		int limit = builder.getLimit();
        int skip = builder.getSkip();
		List<String> propertyNames = builder.getPropertyNames();
		
		List<PaisEntity> paisEntities = paisService.ejecutarConsulta(expression, orderByList);
		
		return () -> {

			List<PaisEdm> filtered = paisEntities.stream().map(entity -> { return new PaisEdm(entity); }).collect(Collectors.toList());
			
			long count = 0;
        	
			if (builder.isCount()) {
                count = filtered.size();

                if (!builder.includeCount()) {
                    return QueryResult.from(count);
                }
            }

            if (skip != 0 || limit != Integer.MAX_VALUE) {
                filtered = filtered.stream().skip(skip).limit(limit).collect(Collectors.toList());
            }

            if (propertyNames != null && !propertyNames.isEmpty()) {
            	try {
            		String jsonInString = GenJsonOdataSelect.generate(propertyNames, filtered);
            		return QueryResult.from(jsonInString);
            	} catch (JsonProcessingException | IllegalArgumentException | IllegalAccessException e) {
                    return QueryResult.from(Collections.emptyList());
                }
            }
            
            QueryResult result = QueryResult.from(filtered);
            if (builder.includeCount()) {
                result = result.withCount(count);
            }
            return result;
		};
	}

}
