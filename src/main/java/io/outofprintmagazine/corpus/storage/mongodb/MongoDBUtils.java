package io.outofprintmagazine.corpus.storage.mongodb;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;


public class MongoDBUtils {

	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(MongoDBUtils.class);
	Properties props;
	private MongoClient client = null;

	private MongoDBUtils() throws IOException {
		super();
		InputStream input = new FileInputStream("data/mongodb.properties");
        props = new Properties();
        props.load(input);
        input.close();
	}
	
	private static MongoDBUtils single_instance = null; 

    public static MongoDBUtils getInstance() throws IOException { 
        if (single_instance == null) 
            single_instance = new MongoDBUtils(); 
  
        return single_instance; 
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
