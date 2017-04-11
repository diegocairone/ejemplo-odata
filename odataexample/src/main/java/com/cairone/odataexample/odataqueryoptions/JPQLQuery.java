package com.cairone.odataexample.odataqueryoptions;

import java.util.Map;

public final class JPQLQuery {

    private final String queryString;
    private final Map<String, Object> queryParams;

    private final int limitCount;
    private final int skipCount;

    public JPQLQuery(String queryString, Map<String, Object> queryParams, int limitCount, int skipCount) {
        this.queryString = queryString;
        this.queryParams = queryParams;
        this.limitCount = limitCount;
        this.skipCount = skipCount;
    }

    public JPQLQuery(String queryString, Map<String, Object> queryParams) {
        this(queryString, queryParams, -1, -1);
    }

    public String getQueryString() {
        return queryString;
    }

    public Map<String, Object> getQueryParams() {
        return queryParams;
    }

    public int getLimitCount() {
        return limitCount;
    }

    public int getSkipCount() {
        return skipCount;
    }

    @Override
    public String toString() {
        return queryString + ", params=" + queryParams;
    }
}
