package com.cairone.odataexample.edm.actions;

import com.cairone.odataexample.EntityServiceRegistar;
import com.sdl.odata.api.edm.annotations.EdmActionImport;

@EdmActionImport(entitySet = "PersonasSectores", action = "SectorAgregar", name = "PersonaSectorAgregarActionImport", namespace = EntityServiceRegistar.NAME_SPACE)
public class PersonaSectorAgregarActionImport {

}
