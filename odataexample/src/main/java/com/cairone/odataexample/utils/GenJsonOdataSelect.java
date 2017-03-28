package com.cairone.odataexample.utils;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sdl.odata.api.edm.annotations.EdmProperty;

public class GenJsonOdataSelect {

	public static String generate(List<String> propertyNames, List<?> collection) throws IllegalArgumentException, IllegalAccessException, JsonProcessingException {
		
		for(Object object : collection) {
			for (Field fld : object.getClass().getDeclaredFields()) {
                EdmProperty ann = fld.getAnnotation(EdmProperty.class);
                if (ann == null || !propertyNames.contains(ann.name())) {
                	fld.setAccessible(true);
                    fld.set(object, null);
                }
            }
		}
		

    	ObjectMapper mapper = new ObjectMapper();
    	mapper.setSerializationInclusion(Include.NON_EMPTY);
    	
    	JsonNode node = mapper.valueToTree(collection);
    	stripEmpty(node);
    	
    	String jsonInString = mapper.writeValueAsString(node);
		
		return jsonInString;
	}
	
	private static void stripEmpty(JsonNode node) {
	    Iterator<JsonNode> it = node.iterator();
	    while (it.hasNext()) {
	        JsonNode child = it.next();
	        if (child.isObject() && child.isEmpty(null))
	            it.remove();
	        else
	            stripEmpty(child);
	    }
	}
}
