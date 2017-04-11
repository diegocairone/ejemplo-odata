package com.cairone.odataexample.odataqueryoptions;

public class JoinString {

    public enum JoinType {
        INNER,
        OUTER
    }

    private final JoinType joinType;
    private final String string;

    public JoinString(JoinType joinType, String string) {
        this.joinType = joinType;
        this.string = string;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public String getString() {
        return string;
    }
}
