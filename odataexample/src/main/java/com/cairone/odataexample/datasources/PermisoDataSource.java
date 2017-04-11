package com.cairone.odataexample.datasources;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.persistence.EntityManagerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.cairone.odataexample.edm.resources.PermisoEdm;
import com.cairone.odataexample.entities.PermisoEntity;
import com.cairone.odataexample.odataqueryoptions.JPQLQuery;
import com.cairone.odataexample.odataqueryoptions.JPQLQueryStrategyBuilder;
import com.cairone.odataexample.odataqueryoptions.JPQLDataSourceProvider;
import com.cairone.odataexample.services.PermisoService;
import com.cairone.odataexample.utils.GenJsonOdataSelect;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.sdl.odata.api.ODataException;
import com.sdl.odata.api.ODataSystemException;
import com.sdl.odata.api.edm.model.EntityDataModel;
import com.sdl.odata.api.parser.ODataUri;
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
public class PermisoDataSource extends JPQLDataSourceProvider implements DataSource {

	@Autowired public PermisoService permisoService = null;

    @Autowired
    private EntityManagerFactory entityManagerFactory;

	@PostConstruct
	public void init() {
		super.entityManagerFactory = entityManagerFactory;
	}

	@Override
	public Object create(ODataUri uri, Object entity, EntityDataModel entityDataModel) throws ODataException {
		throw new ODataSystemException("OPERACION NO PERMITIDA");
	}

	@Override
	public Object update(ODataUri uri, Object entity, EntityDataModel entityDataModel) throws ODataException {
		throw new ODataSystemException("OPERACION NO PERMITIDA");
	}

	@Override
	public void delete(ODataUri uri, EntityDataModel entityDataModel) throws ODataException {
		throw new ODataSystemException("OPERACION NO PERMITIDA");
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
		return requestContext.getEntityDataModel().getType(entityType).getJavaType().equals(PermisoEdm.class);
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

            List<PermisoEntity> permisoEntities = executeQueryListResult(query);
            List<PermisoEdm> filtered = permisoEntities.stream().map(entity -> { return new PermisoEdm(entity); }).collect(Collectors.toList());

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
