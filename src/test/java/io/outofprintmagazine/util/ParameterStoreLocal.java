package io.outofprintmagazine.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.util.IParameterStore;

public class ParameterStoreLocal implements IParameterStore {
	
	private Properties props = new Properties();

	public ParameterStoreLocal() {
		super();
	}
	
	public ParameterStoreLocal(ObjectNode properties) throws IOException {
		this();
		this.init(properties);
	}
	
	public Properties getProperties() {
		return props;
	}

	@Override
	public String getProperty(String name) throws IOException {
		return getProperties().getProperty(name);
	}

	@Override
	public void init(ObjectNode properties) throws IOException {
		Iterator<Entry<String,JsonNode>> fieldNamesIter = properties.fields();
		while (fieldNamesIter.hasNext()) {
			Entry<String,JsonNode> fieldNameObject = fieldNamesIter.next();
			if (!fieldNameObject.getValue().isObject() && !fieldNameObject.getValue().isArray() && !fieldNameObject.getValue().isNull()) {
				getProperties().setProperty(fieldNameObject.getKey(), fieldNameObject.getValue().asText());
			}
		}
	}
}
