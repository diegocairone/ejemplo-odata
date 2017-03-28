package com.cairone.odataexample.dtos;

import com.cairone.odataexample.edm.resources.PaisEdm;


public class PaisFrmDto {

	private Integer id = null;
	private String nombre= null;
	private Integer prefijo = null;

	public PaisFrmDto() {}

	public PaisFrmDto(Integer id, String nombre, Integer prefijo) {
		super();
		this.id = id;
		this.nombre = nombre;
		this.prefijo = prefijo;
	}

	public PaisFrmDto(PaisEdm paisEdm) {
		this.id = paisEdm.getId();
		this.nombre = paisEdm.getNombre() == null || paisEdm.getNombre().trim().isEmpty() ? null : paisEdm.getNombre().trim().toUpperCase();
		this.prefijo = paisEdm.getPrefijo();
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre == null ? null : nombre.trim().toUpperCase();
	}

	public Integer getPrefijo() {
		return prefijo;
	}

	public void setPrefijo(Integer prefijo) {
		this.prefijo = prefijo;
	}
	
}
