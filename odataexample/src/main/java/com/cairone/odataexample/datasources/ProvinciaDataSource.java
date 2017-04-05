package com.cairone.odataexample.datasources;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.persistence.EntityManagerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.MessageSource;
import org.springframework.stereotype.Component;

import scala.Option;

import com.cairone.odataexample.dtos.ProvinciaFrmDto;
import com.cairone.odataexample.dtos.validators.ProvinciaFrmDtoValidator;
import com.cairone.odataexample.edm.resources.ProvinciaEdm;
import com.cairone.odataexample.entities.ProvinciaEntity;
import com.cairone.odataexample.odataqueryoptions.JPAQuery;
import com.cairone.odataexample.odataqueryoptions.JPAQueryStrategyBuilder;
import com.cairone.odataexample.odataqueryoptions.JpaDataSourceProvider;
import com.cairone.odataexample.services.ProvinciaService;
import com.cairone.odataexample.utils.GenJsonOdataSelect;
import com.cairone.odataexample.utils.SQLExceptionParser;
import com.cairone.odataexample.utils.ValidatorUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.sdl.odata.api.ODataException;
import com.sdl.odata.api.ODataSystemException;
import com.sdl.odata.api.edm.model.EntityDataModel;
import com.sdl.odata.api.parser.ODataUri;
import com.sdl.odata.api.parser.ODataUriUtil;
import com.sdl.odata.api.parser.TargetType;
import com.sdl.odata.api.processor.datasource.DataSource;
import com.sdl.odata.api.processor.datasource.ODataDataSourceException;
import com.sdl.odata.api.processor.datasource.TransactionalDataSource;
import com.sdl.odata.api.processor.link.ODataLink;
import com.sdl.odata.api.processor.query.QueryOperation;
import com.sdl.odata.api.processor.query.QueryResult;
import com.sdl.odata.api.processor.query.strategy.QueryOperationStrategy;
import com.sdl.odata.api.service.ODataRequestContext;

@Component
public class ProvinciaDataSource extends JpaDataSourceProvider implements DataSource {
	
	@Autowired private ProvinciaService provinciaService = null;
	@Autowired private ProvinciaFrmDtoValidator provinciaFrmDtoValidator = null;

    @Autowired
    private EntityManagerFactory entityManagerFactory;

	@Autowired
	private MessageSource messageSource = null;

	@PostConstruct
	public void init() {
		super.entityManagerFactory = entityManagerFactory;
	}
	
	@Override
	public Object create(ODataUri uri, Object entity, EntityDataModel entityDataModel) throws ODataException {

		if(entity instanceof ProvinciaEdm) {
			
			ProvinciaEdm provinciaEdm = (ProvinciaEdm) entity;
			ProvinciaFrmDto provinciaFrmDto = new ProvinciaFrmDto(provinciaEdm);

			ValidatorUtil.validate(provinciaFrmDtoValidator, messageSource, provinciaFrmDto);

			try {
				ProvinciaEntity provinciaEntity = provinciaService.nuevo(provinciaFrmDto);
				return new ProvinciaEdm(provinciaEntity);
			} catch (Exception e) {
				String message = SQLExceptionParser.parse(e);
				throw new ODataDataSourceException(message);
			}
		}
		
		throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD PROVINCIA");
	}

	@Override
	public Object update(ODataUri uri, Object entity, EntityDataModel entityDataModel) throws ODataException {

		if(entity instanceof ProvinciaEdm) {

    		Map<String, Object> oDataUriKeyValues = ODataUriUtil.asJavaMap(ODataUriUtil.getEntityKeyMap(uri, entityDataModel));
    					
			ProvinciaEdm provinciaEdm = (ProvinciaEdm) entity;
			ProvinciaFrmDto provinciaFrmDto = new ProvinciaFrmDto(provinciaEdm);

			ValidatorUtil.validate(provinciaFrmDtoValidator, messageSource, provinciaFrmDto);

			Integer paisID = Integer.valueOf(oDataUriKeyValues.get("paisId").toString());
			Integer provinciaID = Integer.valueOf(oDataUriKeyValues.get("id").toString());
			
			provinciaFrmDto.setId(provinciaID);
			provinciaFrmDto.setPaisID(paisID);

			try {
				ProvinciaEntity provinciaEntity = provinciaService.nuevo(provinciaFrmDto);
				return new ProvinciaEdm(provinciaEntity);
			} catch (Exception e) {
				String message = SQLExceptionParser.parse(e);
				throw new ODataDataSourceException(message);
			}
		}
		
		throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD PROVINCIA");
	}
	
	@Override
	public void delete(ODataUri uri, EntityDataModel entityDataModel) throws ODataException {
				
		Option<Object> entity = ODataUriUtil.extractEntityWithKeys(uri, entityDataModel);
    	
    	if(entity.isDefined()) {
    		
    		ProvinciaEdm provincia = (ProvinciaEdm) entity.get();

    		Integer provinciaID = provincia.getId();
    		Integer paisID = provincia.getPaisId();

    		try {
    			provinciaService.borrar(paisID, provinciaID);
				return;
			} catch (Exception e) {
				String message = SQLExceptionParser.parse(e);
				throw new ODataDataSourceException(message);
			}
        }
    	
    	throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD PROVINCIA");
	}

	@Override
	public void createLink(ODataUri uri, ODataLink link, EntityDataModel entityDataModel) throws ODataException {
		// NO HACER NADA
	}

	@Override
	public void deleteLink(ODataUri uri, ODataLink link, EntityDataModel entityDataModel) throws ODataException {
		// NO HACER NADA
	}

	@Override
	public TransactionalDataSource startTransaction() {
		throw new ODataSystemException("No support for transactions");
	}

	@Override
	public boolean isSuitableFor(ODataRequestContext requestContext, String entityType) throws ODataDataSourceException {
		return requestContext.getEntityDataModel().getType(entityType).getJavaType().equals(ProvinciaEdm.class);
	}

	@Override
	public DataSource getDataSource(ODataRequestContext requestContext) {
		return this;
	}
	
	@Override
	public QueryOperationStrategy getStrategy(ODataRequestContext requestContext, QueryOperation operation, TargetType expectedODataEntityType) throws ODataException {

		JPAQueryStrategyBuilder builder = new JPAQueryStrategyBuilder(requestContext);
		
		final JPAQuery query = builder.build(operation);
		List<String> propertyNames = builder.getPropertyNames();
		
        return () -> {

            List<ProvinciaEntity> provinciaEntities = executeQueryListResult(query);
            List<ProvinciaEdm> filtered = provinciaEntities.stream().map(entity -> { return new ProvinciaEdm(entity); }).collect(Collectors.toList());

            long count = 0;
            
            if (builder.isCount()) {
                count = filtered.size();

                if (!builder.isIncludeCount()) {
                    return QueryResult.from(count);
                }
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
            if (builder.isIncludeCount()) {
                result = result.withCount(count);
            }
            
            return result;
        };
	}
}
