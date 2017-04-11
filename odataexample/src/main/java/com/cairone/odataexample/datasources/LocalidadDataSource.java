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

import com.cairone.odataexample.dtos.LocalidadFrmDto;
import com.cairone.odataexample.dtos.validators.LocalidadFrmDtoValidator;
import com.cairone.odataexample.edm.resources.LocalidadEdm;
import com.cairone.odataexample.entities.LocalidadEntity;
import com.cairone.odataexample.odataqueryoptions.JPQLQuery;
import com.cairone.odataexample.odataqueryoptions.JPQLQueryStrategyBuilder;
import com.cairone.odataexample.odataqueryoptions.JPQLDataSourceProvider;
import com.cairone.odataexample.services.LocalidadService;
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
public class LocalidadDataSource extends JPQLDataSourceProvider implements DataSource {

	@Autowired private LocalidadService localidadService = null;
	@Autowired private LocalidadFrmDtoValidator localidadFrmDtoValidator = null;

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

		if(entity instanceof LocalidadEdm) {
			
			LocalidadEdm localidadEdm = (LocalidadEdm) entity;
			LocalidadFrmDto localidadFrmDto = new LocalidadFrmDto(localidadEdm);

			ValidatorUtil.validate(localidadFrmDtoValidator, messageSource, localidadFrmDto);

			try {
				LocalidadEntity localidadEntity = localidadService.nuevo(localidadFrmDto);
				return new LocalidadEdm(localidadEntity);
			} catch (Exception e) {
				String message = SQLExceptionParser.parse(e);
				throw new ODataDataSourceException(message);
			}
		}
		
		throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD LOCALIDAD");
	}

	@Override
	public Object update(ODataUri uri, Object entity, EntityDataModel entityDataModel) throws ODataException {

		if(entity instanceof LocalidadEdm) {

    		Map<String, Object> oDataUriKeyValues = ODataUriUtil.asJavaMap(ODataUriUtil.getEntityKeyMap(uri, entityDataModel));
    					
			LocalidadEdm localidadEdm = (LocalidadEdm) entity;
			LocalidadFrmDto localidadFrmDto = new LocalidadFrmDto(localidadEdm);

			ValidatorUtil.validate(localidadFrmDtoValidator, messageSource, localidadFrmDto);

			Integer localidadID = Integer.valueOf(oDataUriKeyValues.get("localidadId").toString());
			Integer provinciaID = Integer.valueOf(oDataUriKeyValues.get("provinciaId").toString());
			Integer paisID = Integer.valueOf(oDataUriKeyValues.get("paisId").toString());
			
			localidadFrmDto.setPaisId(paisID);
			localidadFrmDto.setProvinciaId(provinciaID);
			localidadFrmDto.setLocalidadId(localidadID);

			try {
				LocalidadEntity localidadEntity = localidadService.nuevo(localidadFrmDto);
				return new LocalidadEdm(localidadEntity);
			} catch (Exception e) {
				String message = SQLExceptionParser.parse(e);
				throw new ODataDataSourceException(message);
			}
		}
		
		throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD LOCALIDAD");
	}

	@Override
	public void delete(ODataUri uri, EntityDataModel entityDataModel) throws ODataException {

		Option<Object> entity = ODataUriUtil.extractEntityWithKeys(uri, entityDataModel);
    	
    	if(entity.isDefined()) {
    		
    		LocalidadEdm localidad = (LocalidadEdm) entity.get();

    		Integer localidadID = localidad.getLocalidadId();
    		Integer provinciaID = localidad.getProvinciaId();
    		Integer paisID = localidad.getPaisId();

    		try {
    			localidadService.borrar(paisID, provinciaID, localidadID);
				return;
			} catch (Exception e) {
				String message = SQLExceptionParser.parse(e);
				throw new ODataDataSourceException(message);
			}
        }
    	
    	throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD LOCALIDAD");
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
		return requestContext.getEntityDataModel().getType(entityType).getJavaType().equals(LocalidadEdm.class);
	}

	@Override
	public DataSource getDataSource(ODataRequestContext requestContext) {
		return this;
	}

	@Override
	public QueryOperationStrategy getStrategy(ODataRequestContext requestContext, QueryOperation operation, TargetType expectedODataEntityType) throws ODataException {
		
		JPQLQueryStrategyBuilder builder = new JPQLQueryStrategyBuilder(requestContext);
		
		final JPQLQuery query = builder.build(operation);
		List<String> propertyNames = builder.getPropertyNames();
		
        return () -> {

            List<LocalidadEntity> localidadEntities = executeQueryListResult(query);
            List<LocalidadEdm> filtered = localidadEntities.stream().map(entity -> { return new LocalidadEdm(entity); }).collect(Collectors.toList());

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
