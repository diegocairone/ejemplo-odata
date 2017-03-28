package com.cairone.odataexample.enums;

import com.sdl.odata.api.edm.annotations.EdmEnum;

@EdmEnum
public enum GeneroEnum {
	MASCULINO('M'), FEMENINO('F');

	private final char valor;
	
	private GeneroEnum(char valor) {
		this.valor = valor;
	}

	public char getValor() {
		return valor;
	}
}
