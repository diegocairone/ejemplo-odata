package com.cairone.odataexample.odataqueryoptions;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdl.odata.api.ODataException;
import com.sdl.odata.api.parser.TargetType;
import com.sdl.odata.api.processor.datasource.DataSource;
import com.sdl.odata.api.processor.datasource.DataSourceProvider;
import com.sdl.odata.api.processor.datasource.ODataDataSourceException;
import com.sdl.odata.api.processor.query.QueryOperation;
import com.sdl.odata.api.processor.query.strategy.QueryOperationStrategy;
import com.sdl.odata.api.service.ODataRequestContext;

public abstract class JPQLDataSourceProvider implements DataSourceProvider {

	private static Logger logger = LoggerFactory.getLogger(JPQLDataSourceProvider.class);
	
	protected EntityManagerFactory entityManagerFactory = null;
	
	@Override
	public abstract boolean isSuitableFor(ODataRequestContext requestContext, String entityType) throws ODataDataSourceException;

	@Override
	public abstract DataSource getDataSource(ODataRequestContext requestContext);

	@Override
	public abstract QueryOperationStrategy getStrategy(ODataRequestContext requestContext, QueryOperation operation, TargetType expectedODataEntityType) throws ODataException;

    @SuppressWarnings("unchecked")
	protected <T> List<T> executeQueryListResult(JPQLQuery jpaQuery) {

        EntityManager em = entityManagerFactory.createEntityManager();

        String queryString = jpaQuery.getQueryString();

    	logger.info("JPQL: {}", queryString);
    	
        Query query = em.createQuery(queryString);
        int nrOfResults = jpaQuery.getLimitCount();
        int startPosition = jpaQuery.getSkipCount();
        Map<String, Object> queryParams = jpaQuery.getQueryParams();

        try {
        	em.getTransaction().begin();
        	
            if (nrOfResults > 0) {
                query.setMaxResults(nrOfResults);
            }

            if (startPosition > 0) {
                query.setFirstResult(startPosition);
            }

            for (Map.Entry<String, Object> entry : queryParams.entrySet()) {
                query.setParameter(entry.getKey(), tryConvert(entry.getValue()));
            }

            return query.getResultList();
        } finally {
            em.close();
        }
    }

    private Object tryConvert(Object parameterType) {
    	
    	if (parameterType instanceof scala.math.BigDecimal) {
            return ((scala.math.BigDecimal) parameterType).intValue();
        }

    	if (parameterType instanceof String && parameterType.toString().matches("\\d{4}-\\d{2}-\\d{2}")) {
            return LocalDate.parse(parameterType.toString());
        }
    	
        return parameterType;
    }
}
