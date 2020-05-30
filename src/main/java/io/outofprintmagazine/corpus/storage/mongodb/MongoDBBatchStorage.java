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

import static com.mongodb.client.model.Filters.eq;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;

import io.outofprintmagazine.corpus.storage.BatchStorage;
import io.outofprintmagazine.util.ParameterStore;


public class MongoDBBatchStorage implements BatchStorage {
	
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(MongoDBBatchStorage.class);
	
	protected Logger getLogger() {
		return logger;
	}
	
	public MongoDBBatchStorage() {
		super();
	}
	
	public MongoDBBatchStorage(ParameterStore parameterStore) {
		this();
		this.setParameterStore(parameterStore);
	}

	private ObjectMapper mapper = new ObjectMapper();
	

	protected ObjectMapper getMapper() {
		return mapper;
	}
	
	private ParameterStore parameterStore;
	
	public ParameterStore getParameterStore() {
		return parameterStore;
	}
	
	@Override
    public void setParameterStore(ParameterStore parameterStore) {
		this.parameterStore = parameterStore;
	}

	@Override
	public ObjectNode listCorpora() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode json = mapper.createObjectNode();
		ArrayNode corporaNode = json.putArray("Corpora");
		MongoCursor<String> cursor = null;
        try {
		    cursor = MongoDBUtils.getInstance(parameterStore).getClient().listDatabaseNames().iterator();
		    while(cursor.hasNext()) {
		    	corporaNode.add(cursor.next());
		    }
        }
        finally {
        	if (cursor != null) {
        		cursor.close();
        	}
        }
		return json;
	}


	@Override
	public void createCorpus(String corpus) throws Exception {
		MongoDatabase mongoDatabase = MongoDBUtils.getInstance(parameterStore).getDatabase(corpus);
		mongoDatabase.getCollection("staging_batches");
		mongoDatabase.getCollection("core_nlp");
		mongoDatabase.getCollection("oop_nlp");
		mongoDatabase.getCollection("pipeline_info");
		mongoDatabase.getCollection("document_texts").createIndex(Indexes.text("data"));
		mongoDatabase.getCollection("scores");
				
	}
	
	public static void main(String[] argv) throws Exception {
		MongoDBBatchStorage me = new MongoDBBatchStorage();
		me.storeStagingBatchString("Submissions", "OOPReading2", "{\n" + 
				"	\"corpusId\" : \"Submissions\",\n" + 
				"	\"corpusBatchId\": \"OOPReading\",\n" + 
				"	\"batchStorageClass\": \"io.outofprintmagazine.corpus.storage.mongodb.MongoDBBatchStorage\",\n" + 
				"	\"scratchStorageClass\": \"io.outofprintmagazine.corpus.storage.s3.S3ScratchStorage\"}");
	}

	@Override
	public ObjectNode listStagingBatches(String corpus) throws Exception {
		ObjectNode json = getMapper().createObjectNode();
		ArrayNode corporaNode = json.putArray("staging_batches");
		MongoCollection<Document> collection = MongoDBUtils.getInstance(parameterStore).getDatabase(corpus).getCollection("staging_batches");
		MongoCursor<Document> cursor = collection.find().iterator();
		try {
		    while (cursor.hasNext()) {
		    	corporaNode.add(cursor.next().getString("corpusBatchId"));

		    }
		} 
		finally {
		    MongoDBUtils.getInstance(parameterStore).closeFinally(cursor);
		}
		return json;
	}

	@Override
	public ObjectNode getStagingBatch(String corpus, String stagingBatchName) throws Exception {
		MongoCollection<Document> collection = MongoDBUtils.getInstance(parameterStore).getDatabase(corpus).getCollection("staging_batches");
		Document document = collection.find(eq("corpusBatchId", stagingBatchName)).first();
		return (ObjectNode) getMapper().readTree(document.toJson());
	}

	@Override
	public void storeStagingBatchJson(String corpus, String stagingBatchName, ObjectNode properties) throws Exception {
		storeStagingBatchString(corpus, stagingBatchName, getMapper().writer().writeValueAsString(properties));
	}

	@Override
	public void storeStagingBatchString(String corpus, String stagingBatchName, String batchContent) throws Exception {
		Document doc = Document.parse(batchContent);
		doc.put("_id", stagingBatchName);
		//Document doc = (Document) JSON.parse(batchContent);
		MongoCollection<Document> collection = MongoDBUtils.getInstance(parameterStore).getDatabase(corpus).getCollection("staging_batches");
		try {
			collection.insertOne(doc);
		}
		catch (MongoWriteException mwe) {
			getLogger().debug(mwe);
			collection.replaceOne(eq("_id", stagingBatchName), doc);
		}

		System.out.println(getStagingBatch(corpus, stagingBatchName));
	}


}
