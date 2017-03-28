package com.cairone.odataexample;

import java.util.Arrays;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.cairone.odataexample.edm.actions.PersonaSectorAgregarAction;
import com.cairone.odataexample.edm.actions.PersonaSectorQuitarAction;
import com.cairone.odataexample.edm.actions.UsuarioPermisoAgregarAction;
import com.cairone.odataexample.edm.actions.UsuarioPermisoQuitarAction;
import com.cairone.odataexample.edm.resources.LocalidadEdm;
import com.cairone.odataexample.edm.resources.PaisEdm;
import com.cairone.odataexample.edm.resources.PermisoEdm;
import com.cairone.odataexample.edm.resources.PersonaEdm;
import com.cairone.odataexample.edm.resources.PersonaSectorEdm;
import com.cairone.odataexample.edm.resources.ProvinciaEdm;
import com.cairone.odataexample.edm.resources.SectorEdm;
import com.cairone.odataexample.edm.resources.TipoDocumentoEdm;
import com.cairone.odataexample.edm.resources.UsuarioEdm;
import com.cairone.odataexample.enums.GeneroEnum;
import com.sdl.odata.api.ODataException;
import com.sdl.odata.api.edm.registry.ODataEdmRegistry;

@Component
public class EntityServiceRegistar {

	public static final String NAME_SPACE = "com.cairone.odataexample";
	public static final String CONTAINER_NAME = "ODataExample";
	
	@Autowired private ODataEdmRegistry oDataEdmRegistry = null;

	@PostConstruct
    public void registerEntities() throws ODataException {
		oDataEdmRegistry.registerClasses(Arrays.asList(
				GeneroEnum.class,
				LocalidadEdm.class,
				PaisEdm.class,
				PermisoEdm.class,
				PersonaEdm.class,
				PersonaSectorEdm.class,
				PersonaSectorAgregarAction.class,
				PersonaSectorQuitarAction.class,
				ProvinciaEdm.class,
				TipoDocumentoEdm.class,
				SectorEdm.class,
				UsuarioEdm.class,
				UsuarioPermisoAgregarAction.class,
				UsuarioPermisoQuitarAction.class));
    }
}
