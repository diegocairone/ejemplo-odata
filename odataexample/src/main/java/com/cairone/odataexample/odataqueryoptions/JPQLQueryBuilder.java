package com.cairone.odataexample.odataqueryoptions;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * JPA query builder.
 *
 * See http://docs.oracle.com/cd/E17904_01/apirefs.1111/e13946/ejb3_langref.html
 * 
 */
public final class JPQLQueryBuilder {
	
	private List<String> selectList = new ArrayList<>();
    private boolean distinct;

    private String fromCollection;
    private String fromAlias;

    private List<JoinString> joinStrings = new ArrayList<>();
    private List<String> expandFields = new ArrayList<>();

    private String whereClause;

    private List<String> orderByFields = new ArrayList<>();

    private int limitCount;
    private int skipCount;

    private Map<String, Object> params = new HashMap<>();

    public List<String> getSelectList() {
        return selectList;
    }

    public JPQLQueryBuilder setSelectList(List<String> selectList) {
        this.selectList = selectList;
        return this;
    }

    public JPQLQueryBuilder addToSelectList(String name) {
        this.selectList.add(name);
        return this;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public JPQLQueryBuilder setDistinct(boolean distinct) {
        this.distinct = distinct;
        return this;
    }

    public String getFromCollection() {
        return fromCollection;
    }

    public JPQLQueryBuilder setFromCollection(String fromCollection) {
        this.fromCollection = fromCollection;
        return this;
    }

    public String getFromAlias() {
        return fromAlias;
    }

    public JPQLQueryBuilder setFromAlias(String fromAlias) {
        this.fromAlias = fromAlias;
        return this;
    }

    public List<JoinString> getJoinStrings() {
        return joinStrings;
    }

    public JPQLQueryBuilder addJoinString(JoinString joinString) {
        this.joinStrings.add(joinString);
        return this;
    }

    public JPQLQueryBuilder addJoinStrings(List<JoinString> joinStrings) {
        this.joinStrings.addAll(joinStrings);
        return this;
    }

    public List<String> getExpandFields() {
        return expandFields;
    }

    public JPQLQueryBuilder addExpandField(String expandField) {
        this.expandFields.add(expandField);
        return this;
    }

    public JPQLQueryBuilder addExpandFields(List<String> expandFields) {
        this.expandFields.addAll(expandFields);
        return this;
    }

    public String getWhereClause() {
        return whereClause;
    }

    public JPQLQueryBuilder setWhereClause(String whereClause) {
        this.whereClause = whereClause;
        return this;
    }

    public List<String> getOrderByFields() {
        return orderByFields;
    }

    public JPQLQueryBuilder addOrderByField(String orderByField) {
        this.orderByFields.add(orderByField);
        return this;
    }

    public JPQLQueryBuilder addOrderByFields(List<String> orderByFields) {
        this.orderByFields.addAll(orderByFields);
        return this;
    }

    public int getLimitCount() {
        return limitCount;
    }

    public JPQLQueryBuilder setLimitCount(int limitCount) {
        this.limitCount = limitCount;
        return this;
    }

    public int getSkipCount() {
        return skipCount;
    }

    public JPQLQueryBuilder setSkipCount(int skipCount) {
        this.skipCount = skipCount;
        return this;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public JPQLQueryBuilder addParams(Map<String, Object> params) {
        this.params.putAll(params);
        return this;
    }

    public JPQLQueryBuilder addParam(String name, Object value) {
        this.params.put(name, value);
        return this;
    }

    public JPQLQuery build() {
    	
        StringBuilder queryStringBuilder = new StringBuilder();

        // SELECT [DISTINCT]
        queryStringBuilder.append("SELECT ");
        
        if (isDistinct()) {
            queryStringBuilder.append("DISTINCT ");
        }

        if (!selectList.isEmpty()) {
            Joiner.on(',').appendTo(queryStringBuilder, selectList);
        } else {
            queryStringBuilder.append(fromAlias);
        }

        // FROM <fromCollection> <fromAlias>
        queryStringBuilder.append(" FROM ").append(fromCollection).append(' ').append(fromAlias);

        // [LEFT] JOIN ...
        if (!joinStrings.isEmpty()) {
            for (JoinString joinString : joinStrings) {
                if (joinString.getJoinType() == JoinString.JoinType.OUTER) {
                    queryStringBuilder.append(" LEFT");
                }
                queryStringBuilder.append(" JOIN ").append(joinString.getString());
            }
        }

        // JOIN FETCH ...
        if (!expandFields.isEmpty()) {
            for (String expandField : expandFields) {
                queryStringBuilder.append(" LEFT JOIN FETCH ").append(expandField);
            }
        }

        // WHERE ...
        if (!Strings.isNullOrEmpty(whereClause)) {
            queryStringBuilder.append(" WHERE ").append(whereClause);
        }

        if (!orderByFields.isEmpty()) {
            queryStringBuilder.append(" ORDER BY ");
            Joiner.on(',').appendTo(queryStringBuilder, orderByFields);
        }

        return new JPQLQuery(queryStringBuilder.toString(), params, limitCount, skipCount);
    }
}
