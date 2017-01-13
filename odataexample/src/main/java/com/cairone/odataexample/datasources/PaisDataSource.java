package com.cairone.odataexample.datasources;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;
import org.springframework.validation.BindingResult;
import org.springframework.validation.DataBinder;

import scala.Option;

import com.cairone.odataexample.dtos.PaisFrmDto;
import com.cairone.odataexample.dtos.validators.PaisFrmDtoValidator;
import com.cairone.odataexample.edm.resources.PaisEdm;
import com.cairone.odataexample.entities.PaisEntity;
import com.cairone.odataexample.repositories.PaisRepository;
import com.cairone.odataexample.strategyBuilders.PaisesStrategyBuilder;
import com.mysema.query.types.expr.BooleanExpression;
import com.sdl.odata.api.ODataException;
import com.sdl.odata.api.ODataSystemException;
import com.sdl.odata.api.edm.model.EntityDataModel;
import com.sdl.odata.api.edm.util.EdmUtil;
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

	@Autowired private PaisRepository paisRepository = null;
	@Autowired private PaisFrmDtoValidator paisFrmDtoValidator = null;
	
	@Override
	public Object create(ODataUri uri, Object entity, EntityDataModel entityDataModel) throws ODataException {
		
		if(entity instanceof PaisEdm) {
			
			PaisEdm paisEdm = (PaisEdm) entity;
    		PaisFrmDto paisFrmDto = new PaisFrmDto(paisEdm);

    		DataBinder binder = new DataBinder(paisFrmDto);
			
			binder.setValidator(paisFrmDtoValidator);
			binder.validate();
			
			BindingResult bindingResult = binder.getBindingResult();
						
			if(bindingResult.hasFieldErrors()) {
				throw new ODataDataSourceException("HAY DATOS INVALIDOS EN LA SOLICITUD ENVIADA");
			}
			
			PaisEntity paisEntity = new PaisEntity();

			paisEntity.setId(paisFrmDto.getId());
    		paisEntity.setNombre(paisFrmDto.getNombre());
    		paisEntity.setPrefijo(paisFrmDto.getPrefijo());
    		
			paisRepository.save(paisEntity);
			
			return new PaisEdm(paisEntity);
		}
		
		throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD PAIS");
	}

	@Override
	public Object update(ODataUri uri, Object entity, EntityDataModel entityDataModel) throws ODataException {

    	if(entity instanceof PaisEdm) {
    		
    		Map<String, Object> oDataUriKeyValues = ODataUriUtil.asJavaMap(ODataUriUtil.getEntityKeyMap(uri, entityDataModel));
    		
    		PaisEdm pais = (PaisEdm) entity;
    		
    		oDataUriKeyValues.values().forEach(item -> {
    			pais.setId(Integer.valueOf( item.toString() ));
    		});
    		
    		PaisFrmDto paisFrmDto = new PaisFrmDto(pais);

    		DataBinder binder = new DataBinder(paisFrmDto);
			
			binder.setValidator(paisFrmDtoValidator);
			binder.validate();
			
			BindingResult bindingResult = binder.getBindingResult();
						
			if(bindingResult.hasFieldErrors()) {
				throw new ODataDataSourceException("HAY DATOS INVALIDOS EN LA SOLICITUD ENVIADA");
			}

        	Integer paisID = pais.getId();
        	PaisEntity paisEntity = paisRepository.findOne(paisID);

    		if(paisEntity == null) {
    			throw new ODataDataSourceException(String.format("NO SE ENCUENTRA UN PAIS CON ID %s", pais.getId()));
    		}
    		
    		paisEntity.setNombre(paisFrmDto.getNombre());
    		paisEntity.setPrefijo(paisFrmDto.getPrefijo());
    		
    		paisRepository.save(paisEntity);
    		
    		return new PaisEdm(paisEntity);
    	}
    	
    	throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD PAIS");
	}

	@Override
	public void delete(ODataUri uri, EntityDataModel entityDataModel) throws ODataException {
		
		Option<Object> entity = ODataUriUtil.extractEntityWithKeys(uri, entityDataModel);
    	
    	if(entity.isDefined()) {
    		
    		PaisEdm pais = (PaisEdm) entity.get();
    		PaisEntity paisEntity = paisRepository.findOne(pais.getId());
            
    		if(paisEntity == null) {
    			throw new ODataDataSourceException(String.format("NO SE ENCUENTRA UN PAIS CON ID %s", pais.getId()));
    		}
    		
    		paisRepository.delete(paisEntity);
    		
    		return;
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
		
		List<PaisEntity> paisEntities = (List<PaisEntity>) ( orderByList == null || orderByList.size() == 0 ?
				paisRepository.findAll(expression) :
				paisRepository.findAll(expression, new Sort(orderByList)) );
		
		return () -> {

			List<PaisEdm> filtered = paisEntities.stream().map(entity -> { return new PaisEdm(entity); }).collect(Collectors.toList());
			
			long count = 0;
        	
            if (builder.isCount() || builder.includeCount()) {
                count = filtered.size();

                if (builder.isCount()) {
                    return QueryResult.from(count);
                }
            }

            if (skip != 0 || limit != Integer.MAX_VALUE) {
                filtered = filtered.stream().skip(skip).limit(limit).collect(Collectors.toList());
            }

            if (propertyNames != null && !propertyNames.isEmpty()) {
                try {
                    return QueryResult.from(EdmUtil.getEdmPropertyValue(filtered.get(0), propertyNames.get(0)));
                } catch (IllegalAccessException e) {
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