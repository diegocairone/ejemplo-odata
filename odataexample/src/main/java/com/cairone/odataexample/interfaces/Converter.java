package com.cairone.odataexample.interfaces;

public interface Converter<X,Y> {

	public Y convertToAnotherAttribute (X anotherAttribute);
}
