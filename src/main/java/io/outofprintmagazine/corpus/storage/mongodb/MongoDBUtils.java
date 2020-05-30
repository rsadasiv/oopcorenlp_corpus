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
package io.outofprintmagazine.corpus.storage.mongodb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import io.outofprintmagazine.util.ParameterStore;


public class MongoDBUtils {

	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(MongoDBUtils.class);
	Properties props;
	private MongoClient client = null;

	private MongoDBUtils(ParameterStore parameterStore) throws IOException {
		super();
		//InputStream input = new FileInputStream("data/mongodb.properties");
        //props = new Properties();
        //props.load(input);
        //input.close();
		//props = ParameterStore.getInstance().getProperties("data", "mongodb.properties");
		props = new Properties();
		props.setProperty("url", parameterStore.getProperty("mongodb_url"));
		props.setProperty("user", parameterStore.getProperty("mongodb_user"));
		props.setProperty("pwd", parameterStore.getProperty("mongodb_pwd"));
	}
	
	private static Map<ParameterStore, MongoDBUtils> instances = new HashMap<ParameterStore, MongoDBUtils>();
	
	    
    public static MongoDBUtils getInstance(ParameterStore parameterStore) throws IOException { 
        if (instances.get(parameterStore) == null) {
        	MongoDBUtils instance = new MongoDBUtils(parameterStore);
            instances.put(parameterStore, instance);
        }
        return instances.get(parameterStore); 
    }
    
    public MongoClient getClient() throws RuntimeException {
		if (client == null) {
			client = new MongoClient(
						new MongoClientURI(
							String.format(
									props.getProperty("url"),
									props.getProperty("user"),
									props.getProperty("pwd")
							)
						)
			);
		}
		return client;
	}
    
    public MongoDatabase getDatabase(String corpus) {
    	return getClient().getDatabase(corpus);
    }
    
    public void closeFinally(MongoCursor<Document> cursor) {
    	try {
    		if (cursor != null) {
    			cursor.close();
    			cursor = null;
    		}
    	}
    	catch (Exception e) {
    		cursor = null;
    	}    	
    }

}
