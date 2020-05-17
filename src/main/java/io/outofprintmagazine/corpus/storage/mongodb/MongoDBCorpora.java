package io.outofprintmagazine.corpus.storage.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.bson.Document;
import org.mortbay.util.ajax.JSON;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;

import static com.mongodb.client.model.Filters.*;

import io.outofprintmagazine.corpus.storage.CorpusStorage;
import io.outofprintmagazine.corpus.storage.s3.S3Corpora;

public abstract class MongoDBCorpora implements io.outofprintmagazine.corpus.storage.CorpusStorage {

	
	private CorpusStorage scratchStorage = null; //new S3Corpora();
	private ObjectMapper mapper = new ObjectMapper();
	

	protected ObjectMapper getMapper() {
		return mapper;
	}
	
	protected CorpusStorage getScratchStorage() {
		return scratchStorage;
	}
	
	private Properties properties = new Properties();
	
	
	public Properties getProperties() {
		return properties;
	}
	
	public MongoDBCorpora() {
		super();
	}
	
	public MongoDBCorpora(Properties properties) {
		this();
		properties.putAll(properties);
	}

	@Override
	public ObjectNode listCorpora() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode json = mapper.createObjectNode();
		ArrayNode corporaNode = json.putArray("Corpora");
		MongoCursor<String> cursor = null;
        try {
		    cursor = MongoDBUtils.getInstance().getClient().listDatabaseNames().iterator();
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
/*
	@Override
	public void createCorpusText(String corpus, Properties properties, String text) throws Exception {

		MongoDatabase mongoDatabase = MongoDBUtils.getInstance().getClient().getDatabase(corpus);
		MongoCollection<Document> collection = null;
		try {
			collection = mongoDatabase.getCollection("staging_properties");
		}
		catch (IllegalArgumentException e) {
			mongoDatabase.createCollection("staging_properties");
			collection = mongoDatabase.getCollection("staging_properties");
		}
		Document document = new Document();
		document.put("document_id", properties.get(CorpusStorage.DOCUMENT_ID));
		for (String key : properties.stringPropertyNames()) {
			document.put(key, properties.getProperty(key));
		}

		collection.insertOne(document);
		try {
			collection = mongoDatabase.getCollection("staging");
		}
		catch (IllegalArgumentException e) {
			mongoDatabase.createCollection("staging");
			collection = mongoDatabase.getCollection("staging");
		}
		document = new Document();
		document.put("document_id", properties.get(CorpusStorage.DOCUMENT_ID));
		document.put("text",  text);
		collection.insertOne(document);
	}*/

	@Override
	public void createCorpus(String corpus) throws Exception {
		MongoDatabase mongoDatabase = MongoDBUtils.getInstance().getDatabase(corpus);
		mongoDatabase.getCollection("staging_batches");
		mongoDatabase.getCollection("core_nlp");
		mongoDatabase.getCollection("oop_nlp");
		mongoDatabase.getCollection("pipeline_info");
		mongoDatabase.getCollection("document_texts").createIndex(Indexes.text("data"));
		mongoDatabase.getCollection("scores");
				
	}

	@Override
	public ObjectNode listStagingBatches(String corpus) throws Exception {
		ObjectNode json = getMapper().createObjectNode();
		ArrayNode corporaNode = json.putArray("staging_batches");
		MongoCollection<Document> collection = MongoDBUtils.getInstance().getDatabase(corpus).getCollection("staging_batches");
		MongoCursor<Document> cursor = collection.find().iterator();
		try {
		    while (cursor.hasNext()) {
		    	corporaNode.add(cursor.next().getString("corpusBatchId"));

		    }
		} 
		finally {
		    MongoDBUtils.getInstance().closeFinally(cursor);
		}
		return json;
	}

	@Override
	public ObjectNode getStagingBatch(String corpus, String stagingBatchName) throws Exception {
		MongoCollection<Document> collection = MongoDBUtils.getInstance().getDatabase(corpus).getCollection("staging_batches");
		Document document = collection.find(eq("corpusBatchId", stagingBatchName)).first();
		return (ObjectNode) getMapper().readTree(document.toJson());
	}

	@Override
	public void storeStagingBatchJson(String corpus, String stagingBatchName, ObjectNode properties) throws Exception {
		storeStagingBatchString(corpus, stagingBatchName, getMapper().writer().writeValueAsString(properties));
	}

	@Override
	public void storeStagingBatchString(String corpus, String stagingBatchName, String batchContent) throws Exception {
		Document doc = (Document) JSON.parse(batchContent);
		MongoCollection<Document> collection = MongoDBUtils.getInstance().getDatabase(corpus).getCollection("staging_batches");
		collection.insertOne(doc);
	}

	@Override
	public ObjectNode storeScratchFileString(String corpus, String scratchFileName, ObjectNode properties, String in) throws Exception {
		return getScratchStorage().storeScratchFileString(corpus, scratchFileName, properties, in);
	}

	@Override
	public ObjectNode storeScratchFileStream(String corpus, String scratchFileName,	ObjectNode properties, InputStream in) throws Exception {
		return getScratchStorage().storeScratchFileStream(corpus, scratchFileName, properties, in);
	}

	@Override
	public InputStream getScratchFileStream(String corpus, String scratchFileName)
			throws Exception {
		return getScratchStorage().getScratchFileStream(corpus, scratchFileName);
	}

	@Override
	public String getScratchFileString(String corpus, String scratchFileName)
			throws Exception {
		return getScratchStorage().getScratchFileString(corpus, scratchFileName);
	}

	@Override
	public InputStream getScratchFilePropertiesStream(String corpus, String scratchFileName)
			throws Exception {
		return getScratchStorage().getScratchFilePropertiesStream(corpus, scratchFileName);
	}

	@Override
	public ObjectNode storeCoreNLP(String corpus, String stagingBatchName, String scratchFileName,
			ObjectNode properties, ObjectNode in) throws Exception {
		Document doc = (Document) JSON.parse(getMapper().writer().writeValueAsString(in));
		doc.put("corpus_batch_id", stagingBatchName);
		doc.put("document_id", scratchFileName);
		MongoCollection<Document> collection = MongoDBUtils.getInstance().getDatabase(corpus).getCollection("core_nlp");
		collection.insertOne(doc);
		return getScratchStorage().storeCoreNLP(corpus, stagingBatchName, scratchFileName, properties, in);
	}

	@Override
	public ObjectNode getCoreNLP(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		return getScratchStorage().getCoreNLP(corpus, stagingBatchName, scratchFileName);
	}

	@Override
	public ObjectNode storeOOPNLP(String corpus, String stagingBatchName, String scratchFileName, ObjectNode properties,
			ObjectNode in) throws Exception {
		Document doc = (Document) JSON.parse(getMapper().writer().writeValueAsString(in));
		doc.put("corpus_batch_id", stagingBatchName);
		doc.put("document_id", scratchFileName);
		MongoCollection<Document> collection = MongoDBUtils.getInstance().getDatabase(corpus).getCollection("oop_nlp");
		collection.insertOne(doc);
		return getScratchStorage().storeCoreNLP(corpus, stagingBatchName, scratchFileName, properties, in);
	}

	@Override
	public ObjectNode getOOPNLP(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		return getScratchStorage().getOOPNLP(corpus, stagingBatchName, scratchFileName);
	}

	@Override
	public ObjectNode storeAsciiText(String corpus, String stagingBatchName, String scratchFileName,
			ObjectNode properties, String in) throws Exception {
		Document doc = (Document) JSON.parse(getMapper().writer().writeValueAsString(properties));
		doc.put("corpus_batch_id", stagingBatchName);
		doc.put("document_id", scratchFileName);
		doc.put("data", "in");
		MongoCollection<Document> collection = MongoDBUtils.getInstance().getDatabase(corpus).getCollection("document_texts");
		collection.insertOne(doc);
		return getScratchStorage().storeAsciiText(corpus, stagingBatchName, scratchFileName, properties, in);
	}

	@Override
	public String getAsciiText(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		return getScratchStorage().getAsciiText(corpus, stagingBatchName, scratchFileName);
	}

	@Override
	public ObjectNode storePipelineInfo(String corpus, String stagingBatchName, String scratchFileName,
			ObjectNode properties, ObjectNode in) throws Exception {
		Document doc = (Document) JSON.parse(getMapper().writer().writeValueAsString(in));
		doc.put("corpus_batch_id", stagingBatchName);
		doc.put("document_id", scratchFileName);
		MongoCollection<Document> collection = MongoDBUtils.getInstance().getDatabase(corpus).getCollection("pipeline_info");
		collection.insertOne(doc);
		return getScratchStorage().storePipelineInfo(corpus, stagingBatchName, scratchFileName, properties, in);
	}
	
	@Override
	public ObjectNode getPipelineInfo(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		return getScratchStorage().getPipelineInfo(corpus, stagingBatchName, scratchFileName);
	}

	@Override
	public String getFileNameFromPath(String scratchFilePath) {
		return getScratchStorage().getFileNameFromPath(scratchFilePath);
	}

	@Override
	public String trimFileExtension(String scratchFileName) {
		return getScratchStorage().trimFileExtension(scratchFileName);
	}

	@Override
	public String getScratchFilePath(String stagingBatchName, String stagingBatchStepName, String scratchFileName)
			throws Exception {
		return getScratchStorage().getScratchFilePath(stagingBatchName, stagingBatchStepName, scratchFileName);
	}

	@Override
	public ObjectNode getScratchFileProperties(String corpus, String scratchFilePath) throws Exception {
		return getScratchStorage().getScratchFileProperties(corpus, scratchFilePath);
	}
}
