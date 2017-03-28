package com.cairone.odataexample.edm.actions;

import java.util.Map;

import com.cairone.odataexample.datasources.PermisoDataSource;
import com.cairone.odataexample.datasources.UsuarioDataSource;
import com.cairone.odataexample.edm.resources.PermisoEdm;
import com.cairone.odataexample.edm.resources.UsuarioEdm;
import com.cairone.odataexample.entities.PermisoEntity;
import com.cairone.odataexample.entities.UsuarioEntity;
import com.cairone.odataexample.entities.UsuarioPermisoEntity;
import com.cairone.odataexample.services.PermisoService;
import com.cairone.odataexample.services.UsuarioService;
import com.cairone.odataexample.utils.SQLExceptionParser;
import com.sdl.odata.api.ODataBadRequestException;
import com.sdl.odata.api.ODataException;
import com.sdl.odata.api.edm.annotations.EdmAction;
import com.sdl.odata.api.edm.annotations.EdmParameter;
import com.sdl.odata.api.edm.annotations.EdmReturnType;
import com.sdl.odata.api.edm.model.Operation;
import com.sdl.odata.api.parser.ODataUriUtil;
import com.sdl.odata.api.processor.datasource.ODataDataSourceException;
import com.sdl.odata.api.processor.datasource.factory.DataSourceFactory;
import com.sdl.odata.api.service.ODataRequestContext;

@EdmAction(namespace = "Permisos", name = "Quitar", isBound = true) 
@EdmReturnType(type = "com.cairone.odataexample.Permiso")
public class UsuarioPermisoQuitarAction implements Operation<PermisoEdm> {

	@EdmParameter(nullable = false)
	private String id = null;

	@Override
	public PermisoEdm doOperation(ODataRequestContext requestContext, DataSourceFactory dataSourceFactory) throws ODataException {

		boolean isSuitable = requestContext.getEntityDataModel().getType("com.cairone.odataexample.Usuario").getJavaType().equals(UsuarioEdm.class);
		
		if(!isSuitable) {
			throw new ODataBadRequestException("LA OPERACION SOLO ES APLICABLE A LOS PERMISOS");
		}
		
		UsuarioDataSource usuarioDataSource = (UsuarioDataSource) dataSourceFactory.getDataSource(requestContext, "com.cairone.odataexample.Usuario");
		PermisoDataSource permisoDataSource = (PermisoDataSource) dataSourceFactory.getDataSource(requestContext, "com.cairone.odataexample.Permiso");
		
		UsuarioService usuarioService = usuarioDataSource.usuarioService;
		PermisoService permisoService = permisoDataSource.permisoService;

		Map<String, Object> map = ODataUriUtil.asJavaMap( ODataUriUtil.getEntityKeyMap(requestContext.getUri(), requestContext.getEntityDataModel()) );
		
		Object keyTipoDocumentoID = map.get("tipoDocumentoId");
		Object keyNumeroDocumento = map.get("numeroDocumento");
		
		if(keyTipoDocumentoID == null || keyNumeroDocumento == null) {
			throw new ODataBadRequestException("NO SE PUEDEN ENCONTRAR EL MENOS UNA DE LAS CLAVES DE LA ENTIDAD USUARIO");
		}
		
		Integer tipoDocumentoID = Integer.valueOf(keyTipoDocumentoID.toString());
		String numeroDocumento = keyNumeroDocumento.toString();
		
		UsuarioEntity usuarioEntity = usuarioService.buscarPorId(tipoDocumentoID, numeroDocumento);

		if(usuarioEntity == null) {
			throw new ODataBadRequestException(String.format("NO SE PUEDE ENCONTRAR UN USUARIO CON ID [TIPODOCUMENTO=%s,NUMERODOCUMENTO=%s]", tipoDocumentoID, numeroDocumento));
		}
		
		PermisoEntity permisoEntity = permisoService.buscarPorNombre(id);

		if(permisoEntity == null) {
			throw new ODataBadRequestException(String.format("NO SE PUEDE ENCONTRAR UN PERMISO CON ID %s", id));
		}
		
		UsuarioPermisoEntity usuarioPermisoEntity = usuarioService.buscarUnPermisoAsignado(usuarioEntity, permisoEntity);

		if(usuarioPermisoEntity == null) {
			throw new ODataBadRequestException(String.format("EL USUARIO %s NO TIENE ASIGNADO EL PERMISO %s", usuarioEntity, permisoEntity));
		}


		try {
			usuarioService.quitarPermiso(usuarioEntity, permisoEntity);
			return null;
		} catch (Exception e) {
			String message = SQLExceptionParser.parse(e);
			throw new ODataDataSourceException(message);
		}
	}
}
