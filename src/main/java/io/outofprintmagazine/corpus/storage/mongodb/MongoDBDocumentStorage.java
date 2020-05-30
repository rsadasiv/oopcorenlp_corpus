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

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;

import io.outofprintmagazine.corpus.storage.DocumentStorage;
import io.outofprintmagazine.util.ParameterStore;

public class MongoDBDocumentStorage implements DocumentStorage {
	
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(MongoDBBatchStorage.class);
	
	protected Logger getLogger() {
		return logger;
	}
	
	public MongoDBDocumentStorage() {
		super();
	}
	
	public MongoDBDocumentStorage(ParameterStore parameterStore) {
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
	public void storeCoreNLP(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception {
		Document doc = Document.parse(getMapper().writer().writeValueAsString(in));
		doc.put("_id", scratchFileName);
		doc.put("corpus_batch_id", stagingBatchName);
		doc.put("document_id", scratchFileName);
		MongoCollection<Document> collection = MongoDBUtils.getInstance(parameterStore).getDatabase(corpus).getCollection("core_nlp");
		try {
			collection.insertOne(doc);
		}
		catch (MongoWriteException mwe) {
			getLogger().debug(mwe);
			collection.replaceOne(eq("_id", scratchFileName), doc);
		}
	}

	@Override
	public ObjectNode getCoreNLP(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		MongoCollection<Document> collection = MongoDBUtils.getInstance(parameterStore).getDatabase(corpus).getCollection("core_nlp");
		Document document = collection.find(and(eq("corpusBatchId", stagingBatchName), eq("document_id", scratchFileName))).first();
		return (ObjectNode) getMapper().readTree(document.toJson());
	}

	@Override
	public void storeOOPNLP(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception {
		Document doc = Document.parse(getMapper().writer().writeValueAsString(in));
		doc.put("_id", scratchFileName);
		doc.put("corpus_batch_id", stagingBatchName);
		doc.put("document_id", scratchFileName);
		MongoCollection<Document> collection = MongoDBUtils.getInstance(parameterStore).getDatabase(corpus).getCollection("oop_nlp");
		try {
			collection.insertOne(doc);
		}
		catch (MongoWriteException mwe) {
			getLogger().debug(mwe);
			collection.replaceOne(eq("_id", scratchFileName), doc);
		}
	}

	@Override
	public ObjectNode getOOPNLP(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		MongoCollection<Document> collection = MongoDBUtils.getInstance(parameterStore).getDatabase(corpus).getCollection("oop_nlp");
		Document document = collection.find(and(eq("corpusBatchId", stagingBatchName), eq("document_id", scratchFileName))).first();
		return (ObjectNode) getMapper().readTree(document.toJson());
	}

	@Override
	public void storeAsciiText(String corpus, String stagingBatchName, String scratchFileName, String in) throws Exception {
		Document doc = new Document();
		doc.put("_id", scratchFileName);
		doc.put("corpus_batch_id", stagingBatchName);
		doc.put("document_id", scratchFileName);
		doc.put("data", in);
		MongoCollection<Document> collection = MongoDBUtils.getInstance(parameterStore).getDatabase(corpus).getCollection("document_texts");
		try {
			collection.insertOne(doc);
		}
		catch (MongoWriteException mwe) {
			//getLogger().debug(mwe);
			collection.replaceOne(eq("_id", scratchFileName), doc);
		}
	}

	@Override
	public String getAsciiText(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		MongoCollection<Document> collection = MongoDBUtils.getInstance(parameterStore).getDatabase(corpus).getCollection("document_texts");
		Document document = collection.find(and(eq("corpusBatchId", stagingBatchName), eq("document_id", scratchFileName))).first();
		return document.getString("data");
	}

	@Override
	public void storePipelineInfo(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception {
		Document doc = Document.parse(getMapper().writer().writeValueAsString(in));
		doc.put("_id", scratchFileName);
		doc.put("corpus_batch_id", stagingBatchName);
		doc.put("document_id", scratchFileName);
		MongoCollection<Document> collection = MongoDBUtils.getInstance(parameterStore).getDatabase(corpus).getCollection("pipeline_info");
		try {
			collection.insertOne(doc);
		}
		catch (MongoWriteException mwe) {
			getLogger().debug(mwe);
			collection.replaceOne(eq("_id", scratchFileName), doc);
		}
	}
	
	@Override
	public ObjectNode getPipelineInfo(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		MongoCollection<Document> collection = MongoDBUtils.getInstance(parameterStore).getDatabase(corpus).getCollection("pipeline_info");
		Document document = collection.find(and(eq("corpusBatchId", stagingBatchName), eq("document_id", scratchFileName))).first();
		return (ObjectNode) getMapper().readTree(document.toJson());
	}

	@Override
	public void storeOOPAggregates(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in)
			throws Exception {
		Document doc = Document.parse(getMapper().writer().writeValueAsString(in));
		doc.put("_id", scratchFileName);
		doc.put("corpus_batch_id", stagingBatchName);
		doc.put("document_id", scratchFileName);
		MongoCollection<Document> collection = MongoDBUtils.getInstance(parameterStore).getDatabase(corpus).getCollection("oop_nlp_aggregates");
		try {
			collection.insertOne(doc);
		}
		catch (MongoWriteException mwe) {
			getLogger().debug(mwe);
			collection.replaceOne(eq("_id", scratchFileName), doc);
		}
		
	}

	@Override
	public ObjectNode getOOPAggregates(String corpus, String stagingBatchName, String scratchFileName)
			throws Exception {
		MongoCollection<Document> collection = MongoDBUtils.getInstance(parameterStore).getDatabase(corpus).getCollection("oop_nlp_aggregates");
		Document document = collection.find(and(eq("corpusBatchId", stagingBatchName), eq("document_id", scratchFileName))).first();
		return (ObjectNode) getMapper().readTree(document.toJson());
	}
	
	@Override
	public ObjectNode getCorpusAggregateScores(String corpus) throws Exception {
		return null;
	}

	@Override
	public ObjectNode getCorpusIDFScores(String corpus) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void storeOOPZScores(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void storeOOPTfidfScores(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ObjectNode getCorpusMyersBriggsAggregateScores(String corpus) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void storeOOPMyersBriggsScores(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

}
