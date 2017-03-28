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

import com.cairone.odataexample.dtos.UsuarioFrmDto;
import com.cairone.odataexample.dtos.validators.UsuarioFrmDtoValidator;
import com.cairone.odataexample.edm.resources.UsuarioEdm;
import com.cairone.odataexample.entities.UsuarioEntity;
import com.cairone.odataexample.services.UsuarioService;
import com.cairone.odataexample.strategyBuilders.UsuariosStrategyBuilder;
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
public class UsuarioDataSource implements DataSourceProvider, DataSource {

	@Autowired public UsuarioService usuarioService = null;
	@Autowired public UsuarioFrmDtoValidator usuarioFrmDtoValidator = null;

	@Autowired
	private MessageSource messageSource = null;

	@Override
	public Object create(ODataUri uri, Object entity, EntityDataModel entityDataModel) throws ODataException {

		if(entity instanceof UsuarioEdm) {
			
			UsuarioEdm usuarioEdm = (UsuarioEdm) entity;
			UsuarioFrmDto usuarioFrmDto = new UsuarioFrmDto(usuarioEdm);

			ValidatorUtil.validate(usuarioFrmDtoValidator, messageSource, usuarioFrmDto);
			
			try {
				UsuarioEntity usuarioEntity = usuarioService.nuevo(usuarioFrmDto);
				return new UsuarioEdm(usuarioEntity);
			} catch (Exception e) {
				String message = SQLExceptionParser.parse(e);
				throw new ODataDataSourceException(message);
			}
		}
		
		throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD USUARIO");
	}

	@Override
	public Object update(ODataUri uri, Object entity, EntityDataModel entityDataModel) throws ODataException {

		if(entity instanceof UsuarioEdm) {

    		Map<String, Object> oDataUriKeyValues = ODataUriUtil.asJavaMap(ODataUriUtil.getEntityKeyMap(uri, entityDataModel));
    		
    		UsuarioEdm usuarioEdm = (UsuarioEdm) entity;
    		UsuarioFrmDto usuarioFrmDto = new UsuarioFrmDto(usuarioEdm);

			ValidatorUtil.validate(usuarioFrmDtoValidator, messageSource, usuarioFrmDto);
			
    		Integer tipoDocumentoID = Integer.valueOf(oDataUriKeyValues.get("tipoDocumentoId").toString());
    		String numeroDocumento = oDataUriKeyValues.get("numeroDocumento").toString();
    		
    		usuarioEdm.setTipoDocumentoId(tipoDocumentoID);
    		usuarioEdm.setNumeroDocumento(numeroDocumento);

			try {
				UsuarioEntity usuarioEntity = usuarioService.nuevo(usuarioFrmDto);
				return new UsuarioEdm(usuarioEntity);
			} catch (Exception e) {
				String message = SQLExceptionParser.parse(e);
				throw new ODataDataSourceException(message);
			}
		}
		
		throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD USUARIO");
	}

	@Override
	public void delete(ODataUri uri, EntityDataModel entityDataModel) throws ODataException {

		Option<Object> entity = ODataUriUtil.extractEntityWithKeys(uri, entityDataModel);
    	
    	if(entity.isDefined()) {
    		
    		UsuarioEdm usuarioEdm = (UsuarioEdm) entity.get();

    		Integer tipoDocumentoID = usuarioEdm.getTipoDocumentoId();
    		String numeroDocumento = usuarioEdm.getNumeroDocumento();
    		
    		try {
    			usuarioService.borrar(tipoDocumentoID, numeroDocumento);
				return;
			} catch (Exception e) {
				String message = SQLExceptionParser.parse(e);
				throw new ODataDataSourceException(message);
			}
        }
    	
    	throw new ODataDataSourceException("LOS DATOS NO CORRESPONDEN A LA ENTIDAD USUARIO");
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
		return requestContext.getEntityDataModel().getType(entityType).getJavaType().equals(UsuarioEdm.class);
	}

	@Override
	public DataSource getDataSource(ODataRequestContext requestContext) {
		return this;
	}

	@Override
	public QueryOperationStrategy getStrategy(ODataRequestContext requestContext, QueryOperation operation, TargetType expectedODataEntityType) throws ODataException {

		UsuariosStrategyBuilder builder = new UsuariosStrategyBuilder();
		BooleanExpression expression = builder.buildCriteria(operation, requestContext);
		List<Sort.Order> orderByList = builder.getOrderByList();
		
		int limit = builder.getLimit();
        int skip = builder.getSkip();
		List<String> propertyNames = builder.getPropertyNames();
		
		Page<UsuarioEntity> pageUsuarioEntity = usuarioService.ejecutarConsulta(expression, orderByList, limit);
		List<UsuarioEntity> usuarioEntities = pageUsuarioEntity.getContent();
		
		return () -> {

			List<UsuarioEdm> filtered = usuarioEntities.stream().map(entity -> { return new UsuarioEdm(entity); }).collect(Collectors.toList());
			
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
