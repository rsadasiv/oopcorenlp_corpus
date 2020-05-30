/*******************************************************************************
 * Copyright (C) 2020 Ram Sadasiv
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package io.outofprintmagazine.corpus.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.util.ParameterStore;

/*
 * KNOWN PARAMETER FILES
 * ----oopcorenlp
 * --data/azure_api_key.txt
 * azure_apiKey
 * ??azure_apiKey2
 * --data/faceplusplus_api_key.txt
 * faceplusplus_apiKey
 * faceplusplus_secret
 * --data/flickr_api_key.txt
 * flickr_apiKey
 * flickr_secret
 * data/phrasefinder_credentials.properties
 * data/perfecttense.properties
 * data/wikipedia_api_key.txt
 * data/oopcorenlp.properties
 * 
 * --oopcorenlp_corpus
 * data/dropbox.properties
 * data/twitter.properties
 * data/mongodb.properties
 * data/postgresql.properties
 * data/aws.properties
 * data/corpus_bucket.txt
 * data/corpus_directory.txt
 */
//"data", "oopcorenlp.properties"


public class ParameterStorePropertiesFile implements ParameterStore {

	private static final Logger logger = LogManager.getLogger(ParameterStorePropertiesFile.class);
	
	private Properties props = new Properties();
	
	protected Logger getLogger() {
		return logger;
	}
	
	public ParameterStorePropertiesFile() throws IOException {
		super();
    }
	
	public ParameterStorePropertiesFile(String path, String name) throws IOException {
		this();
		this.init(path, name);
    }
	
	public Properties getProperties() {
		return props;
	}
        
    public String getProperty(String name) throws IOException {
    	//return getProperties("data", "oopcorenlp.properties").getProperty(name);
    	return props.getProperty(name);
    }

	@Override
	public void init(ObjectNode properties) throws IOException {
		init(
				properties.get("propertiesFilePath").asText(),
				properties.get("propertiesFileName").asText()
		);
	}
	
	public void init(String path, String name) throws IOException {
    	InputStream input = null;
    	try {
    		input = new FileInputStream(String.format("%s/%s", path, name));
    		props.load(input);
    	}
    	catch (Exception e) {
    		logger.error(e);
    	}
    	finally {
    		if (input != null) {
    			input.close();
    		}
    		input = null;
    	}
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof ParameterStorePropertiesFile) {
			return ((ParameterStorePropertiesFile)o).getProperties().equals(getProperties());
		}
		else {
			return false;
		}
	}
}
