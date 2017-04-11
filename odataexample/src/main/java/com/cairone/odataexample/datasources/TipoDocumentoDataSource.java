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

import com.cairone.odataexample.dtos.TipoDocumentoFrmDto;
import com.cairone.odataexample.dtos.validators.TipoDocumentoFrmDtoValidator;
import com.cairone.odataexample.edm.resources.TipoDocumentoEdm;
import com.cairone.odataexample.entities.TipoDocumentoEntity;
import com.cairone.odataexample.odataqueryoptions.JPQLQuery;
import com.cairone.odataexample.odataqueryoptions.JPQLQueryStrategyBuilder;
import com.cairone.odataexample.odataqueryoptions.JPQLDataSourceProvider;
import com.cairone.odataexample.services.TipoDocumentoService;
import com.cairone.odataexample.utils.GenJsonOdataSelect;
import com.cairone.odataexample.utils.SQLExceptionParser;
import com.cairone.odataexample.utils.ValidatorUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.sdl.odata.api.ODataException;
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
public class TipoDocumentoDataSource extends JPQLDataSourceProvider implements DataSource {

	@Autowired private TipoDocumentoService tipoDocumentoService = null;
	@Autowired private TipoDocumentoFrmDtoValidator tipoDocumentoFrmDtoValidator = null;

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

		if(entity instanceof TipoDocumentoEdm) {
			
			TipoDocumentoEdm tipoDocumentoEdm = (TipoDocumentoEdm) entity;
			TipoDocumentoFrmDto tipoDocumentoFrmDto = new TipoDocumentoFrmDto(tipoDocumentoEdm);

			ValidatorUtil.validate(tipoDocumentoFrmDtoValidator, messageSource, tipoDocumentoFrmDto);
			
			try {
				TipoDocumentoEntity tipoDocumentoEntity = tipoDocumentoService.nuevo(tipoDocumentoFrmDto);
				return new TipoDocumentoEdm(tipoDocumentoEntity);
			} catch (Exception e) {
				String message = SQLExceptionParser.parse(e);
				throw new ODataDataSourceException(message);
			}
		}
		
		throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD TIPO DE DOCUMENTO");
	}

	@Override
	public Object update(ODataUri uri, Object entity, EntityDataModel entityDataModel) throws ODataException {

		if(entity instanceof TipoDocumentoEdm) {

    		Map<String, Object> oDataUriKeyValues = ODataUriUtil.asJavaMap(ODataUriUtil.getEntityKeyMap(uri, entityDataModel));
    		
    		TipoDocumentoEdm tipoDocumentoEdm = (TipoDocumentoEdm) entity;
    		TipoDocumentoFrmDto tipoDocumentoFrmDto = new TipoDocumentoFrmDto(tipoDocumentoEdm);

			ValidatorUtil.validate(tipoDocumentoFrmDtoValidator, messageSource, tipoDocumentoFrmDto);
			
    		Integer tipoDocumentoID = Integer.valueOf(oDataUriKeyValues.get("id").toString());
    		tipoDocumentoFrmDto.setId(tipoDocumentoID);

			try {
				TipoDocumentoEntity tipoDocumentoEntity = tipoDocumentoService.actualizar(tipoDocumentoFrmDto);
				return new TipoDocumentoEdm(tipoDocumentoEntity);
			} catch (Exception e) {
				String message = SQLExceptionParser.parse(e);
				throw new ODataDataSourceException(message);
			}
		}
		
		throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD TIPO DE DOCUMENTO");
	}

	@Override
	public void delete(ODataUri uri, EntityDataModel entityDataModel) throws ODataException {
		
		Option<Object> entity = ODataUriUtil.extractEntityWithKeys(uri, entityDataModel);
    	
    	if(entity.isDefined()) {
    		
    		TipoDocumentoEdm tipoDocumentoEdm = (TipoDocumentoEdm) entity.get();
    		Integer tipoDocumentoID = tipoDocumentoEdm.getId();
    		
    		try {
				tipoDocumentoService.borrar(tipoDocumentoID);
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
		
	}

	@Override
	public void deleteLink(ODataUri uri, ODataLink link, EntityDataModel entityDataModel) throws ODataException {
		
	}

	@Override
	public TransactionalDataSource startTransaction() {
		return null;
	}

	@Override
	public boolean isSuitableFor(ODataRequestContext requestContext, String entityType) throws ODataDataSourceException {
		return requestContext.getEntityDataModel().getType(entityType).getJavaType().equals(TipoDocumentoEdm.class);
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

            List<TipoDocumentoEntity> tipoDocumentoEntities = executeQueryListResult(query);
            List<TipoDocumentoEdm> filtered = tipoDocumentoEntities.stream().map(entity -> { return new TipoDocumentoEdm(entity); }).collect(Collectors.toList());

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
