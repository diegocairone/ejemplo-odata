package com.cairone.odataexample.datasources;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.MessageSource;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

import scala.Option;

import com.cairone.odataexample.dtos.PersonaFrmDto;
import com.cairone.odataexample.dtos.validators.PersonaFrmDtoValidator;
import com.cairone.odataexample.edm.resources.PersonaEdm;
import com.cairone.odataexample.entities.PersonaEntity;
import com.cairone.odataexample.services.PersonaService;
import com.cairone.odataexample.strategyBuilders.PersonasStrategyBuilder;
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
public class PersonaDataSource implements DataSourceProvider, DataSource {

	@Autowired public PersonaService personaService = null;
	@Autowired private PersonaFrmDtoValidator personaFrmDtoValidator = null;

	@Autowired
	private MessageSource messageSource = null;

	@Override
	public Object create(ODataUri uri, Object entity, EntityDataModel entityDataModel) throws ODataException {

		if(entity instanceof PersonaEdm) {
			
			PersonaEdm personaEdm = (PersonaEdm) entity;
			PersonaFrmDto personaFrmDto = new PersonaFrmDto(personaEdm);

			ValidatorUtil.validate(personaFrmDtoValidator, messageSource, personaFrmDto);
			
			try {
				PersonaEntity personaEntity = personaService.nuevo(personaFrmDto);
				return new PersonaEdm(personaEntity);
			} catch (Exception e) {
				String message = SQLExceptionParser.parse(e);
				throw new ODataDataSourceException(message);
			}
		}
		
		throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD PERSONA");
	}

	@Override
	public Object update(ODataUri uri, Object entity, EntityDataModel entityDataModel) throws ODataException {

		if(entity instanceof PersonaEdm) {

    		Map<String, Object> oDataUriKeyValues = ODataUriUtil.asJavaMap(ODataUriUtil.getEntityKeyMap(uri, entityDataModel));
    		
    		PersonaEdm personaEdm = (PersonaEdm) entity;
    		PersonaFrmDto personaFrmDto = new PersonaFrmDto(personaEdm);

    		ValidatorUtil.validate(personaFrmDtoValidator, messageSource, personaFrmDto);
			
    		Integer tipoDocumentoID = Integer.valueOf(oDataUriKeyValues.get("tipoDocumentoId").toString());
    		String numeroDocumento = oDataUriKeyValues.get("numeroDocumento").toString();
    		
    		personaEdm.setTipoDocumentoId(tipoDocumentoID);
    		personaEdm.setNumeroDocumento(numeroDocumento);

			try {
				PersonaEntity personaEntity = personaService.actualizar(personaFrmDto);
				return new PersonaEdm(personaEntity);
			} catch (Exception e) {
				String message = SQLExceptionParser.parse(e);
				throw new ODataDataSourceException(message);
			}
		}
		
		throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD PERSONA");
	}

	@Override
	public void delete(ODataUri uri, EntityDataModel entityDataModel) throws ODataException {

		Option<Object> entity = ODataUriUtil.extractEntityWithKeys(uri, entityDataModel);
    	
    	if(entity.isDefined()) {
    		
    		PersonaEdm personaEdm = (PersonaEdm) entity.get();

    		Integer tipoDocumentoID = personaEdm.getTipoDocumentoId();
    		String numeroDocumento = personaEdm.getNumeroDocumento();
    		
    		try {
    			personaService.borrar(tipoDocumentoID, numeroDocumento);
				return;
			} catch (Exception e) {
				String message = SQLExceptionParser.parse(e);
				throw new ODataDataSourceException(message);
			}
        }
    	
    	throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD SECTOR");
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
		return requestContext.getEntityDataModel().getType(entityType).getJavaType().equals(PersonaEdm.class);
	}

	@Override
	public DataSource getDataSource(ODataRequestContext requestContext) {
		return this;
	}

	@Override
	public QueryOperationStrategy getStrategy(ODataRequestContext requestContext, QueryOperation operation, TargetType expectedODataEntityType) throws ODataException {

		PersonasStrategyBuilder builder = new PersonasStrategyBuilder();
		BooleanExpression expression = builder.buildCriteria(operation, requestContext);
		List<Sort.Order> orderByList = builder.getOrderByList();
		
		int limit = builder.getLimit();
        int skip = builder.getSkip();
		List<String> propertyNames = builder.getPropertyNames();
		
		Page<PersonaEntity> pagePersonaEntity = personaService.ejecutarConsulta(expression, orderByList, limit);
		List<PersonaEntity> personaEntities = pagePersonaEntity.getContent();
		
		return () -> {

			List<PersonaEdm> filtered = personaEntities.stream().map(entity -> { return new PersonaEdm(entity); }).collect(Collectors.toList());
			
			long count = 0;
        	
            if (builder.isCount() || builder.includeCount()) {
                count = filtered.size();

                if (builder.isCount()) {
                    return QueryResult.from(count);
                }
            }

            if (skip != 0 || limit != Integer.MAX_VALUE) {
                filtered = filtered.stream().skip(skip).collect(Collectors.toList());
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
