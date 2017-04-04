package com.cairone.odataexample.converters;

import com.cairone.odataexample.interfaces.Converter;
import com.mysema.query.types.Operator;
import com.mysema.query.types.Ops;

public class QueryDslOpsConverter implements Converter<String, Operator<Boolean>> {

	@Override
	public Operator<Boolean> convertToAnotherAttribute(String anotherAttribute) {
		
		switch(anotherAttribute) {
		case "=":
			return Ops.EQ;
		case "<>":
			return Ops.NE;
		case "<":
			return Ops.LT;
		case "<=":
			return Ops.LOE;
		case ">":
			return Ops.GT;
		case ">=":
			return Ops.GOE;
		}
		return null;
	}
}
