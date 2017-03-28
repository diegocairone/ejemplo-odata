package com.cairone.odataexample.edm.actions;

import java.time.LocalDate;
import java.util.Map;

import com.cairone.odataexample.datasources.PersonaDataSource;
import com.cairone.odataexample.datasources.SectorDataSource;
import com.cairone.odataexample.edm.resources.PersonaEdm;
import com.cairone.odataexample.edm.resources.PersonaSectorEdm;
import com.cairone.odataexample.entities.PersonaEntity;
import com.cairone.odataexample.entities.PersonaSectorEntity;
import com.cairone.odataexample.entities.SectorEntity;
import com.cairone.odataexample.services.PersonaService;
import com.cairone.odataexample.services.SectorService;
import com.cairone.odataexample.utils.FechaUtil;
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

@EdmAction(namespace = "Sectores", name = "Agregar", isBound = true) 
@EdmReturnType(type = "com.cairone.odataexample.PersonaSector")
public class PersonaSectorAgregarAction implements Operation<PersonaSectorEdm> {

	@EdmParameter(nullable = false)
	private Integer sectorId = null;

	@EdmParameter(nullable = false)
	private LocalDate fechaIngreso = null;
	
	@Override
	public PersonaSectorEdm doOperation(ODataRequestContext requestContext, DataSourceFactory dataSourceFactory) throws ODataException {

		boolean isSuitable = requestContext.getEntityDataModel().getType("com.cairone.odataexample.Persona").getJavaType().equals(PersonaEdm.class);
		
		if(!isSuitable) {
			throw new ODataBadRequestException("LA OPERACION SOLO ES APLICABLE A LAS PERSONAS");
		}
		
		SectorDataSource sectorDataSource = (SectorDataSource) dataSourceFactory.getDataSource(requestContext, "com.cairone.odataexample.Sector");
		PersonaDataSource personaDataSource = (PersonaDataSource) dataSourceFactory.getDataSource(requestContext, "com.cairone.odataexample.Persona");
		
		SectorService sectorService = sectorDataSource.sectorService;
		PersonaService personaService = personaDataSource.personaService;
		
		Map<String, Object> map = ODataUriUtil.asJavaMap( ODataUriUtil.getEntityKeyMap(requestContext.getUri(), requestContext.getEntityDataModel()) );
		
		Object keyTipoDocumentoID = map.get("tipoDocumentoId");
		Object keyNumeroDocumento = map.get("numeroDocumento");
		
		if(keyTipoDocumentoID == null || keyNumeroDocumento == null) {
			throw new ODataBadRequestException("NO SE PUEDEN ENCONTRAR EL MENOS UNA DE LAS CLAVES DE LA ENTIDAD PERSONA");
		}
		
		Integer tipoDocumentoID = Integer.valueOf(keyTipoDocumentoID.toString());
		String numeroDocumento = keyNumeroDocumento.toString();
		
		PersonaEntity personaEntity = personaService.buscarPorId(tipoDocumentoID, numeroDocumento);
		
		if(personaEntity == null) {
			throw new ODataBadRequestException(String.format("NO SE PUEDE ENCONTRAR UNA PERSONA CON ID [TIPODOCUMENTO=%s,NUMERODOCUMENTO=%s]", tipoDocumentoID, numeroDocumento));
		}
		
		SectorEntity sectorEntity = sectorService.buscarPorID(sectorId);

		if(sectorEntity == null) {
			throw new ODataBadRequestException(String.format("NO SE PUEDE ENCONTRAR UN SECTOR CON ID %s", sectorId));
		}
		
		PersonaSectorEntity personaSectorEntity = personaService.buscarIngresoEnSector(personaEntity, sectorEntity);
		
		if(personaSectorEntity != null) {
			throw new ODataBadRequestException(String.format("LA PERSONA %s YA EXISTE EN EL SECTOR %s", personaEntity, sectorEntity));
		}
		
		try {
			personaSectorEntity = personaService.ingresarSector(personaEntity, sectorEntity, FechaUtil.asDate(fechaIngreso));
			PersonaSectorEdm personaSectorEdm = new PersonaSectorEdm(personaSectorEntity);
			return personaSectorEdm;
		} catch (Exception e) {
			String message = SQLExceptionParser.parse(e);
			throw new ODataDataSourceException(message);
		}
	}
}
