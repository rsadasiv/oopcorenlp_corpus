package io.outofprintmagazine.corpus.storage.s3;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.storage.BatchStorage;

public class S3BatchStorage implements BatchStorage {

	//extends FileCorpora
	//Path=//
	//Bucket=

	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(S3BatchStorage.class);
	
	protected Logger getLogger() {
		return logger;
	}
	
	public static final String defaultBucket = "oop-corpora";
	
	protected ObjectMapper getMapper() {
		return mapper; 
	}
	
	protected ObjectMapper mapper;
	
	public String getDefaultPath() {
		return "Test";
	}
	private Properties properties = new Properties();
	
	public Properties getProperties() {
		return properties;
	}
	
	public S3BatchStorage() {
		super();
		mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		mapper.configure(com.fasterxml.jackson.core.JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
	}
	
	public S3BatchStorage(Properties properties) {
		this();
		properties.putAll(properties);
	}
	
	@Override
	public void createCorpus(String corpus) throws Exception {
		//pass
	}

	@Override
	public ObjectNode listCorpora() throws Exception  {

		ObjectNode json = getMapper().createObjectNode();
		ArrayNode corporaNode = json.putArray("Corpora");

		for (S3ObjectSummary objectSummary: S3Utils.getInstance().getS3Client().listObjects(
				properties.getProperty("Bucket", defaultBucket), 
				properties.getProperty("Path", getDefaultPath())
			).getObjectSummaries()) {
			if (objectSummary.getKey().endsWith("/")) {
				corporaNode.add(objectSummary.getKey().substring(0, objectSummary.getKey().length() - 1));
			}
		}
		return json;
	}
	
	protected String getCorpusPath(String corpus) {
		String path = (
				getProperties().getProperty("Path", getDefaultPath())
				+ "/"	
				+ corpus
		);

		return path;
		
	}
	
	protected String getCorpusStagingBatchPath(String corpus, String stagingBatchName) {
		String path = (
				getCorpusPath(corpus) 
				+ "/" 
				+ stagingBatchName
		);

		return path;
	}
	
	protected String getCorpusStagingBatchItemPath(String corpus, String stagingBatchName, String stagingBatchItemName) {
		String path = (
				getCorpusPath(corpus) 
				+ "/" 
				+ stagingBatchName
				+ "/" 
				+ stagingBatchItemName
		);

		return path;
	}
	

	protected String getCorpusStagingBatchItemPropertiesPath(String corpus, String stagingBatchName, String stagingBatchItemName) {
		return (
				getCorpusStagingBatchItemPath(corpus, stagingBatchName, stagingBatchItemName)
				+ "/" 
				+ "BatchItemProperties.json"
		);
	}	
	
	protected String getCorpusStagingBatchScratchPath(String corpus, String stagingBatchName) {
		String path = (
				getCorpusStagingBatchPath(corpus, stagingBatchName) 
				+ "/" 
				+ "Scratch"
		);

		return path;
	}
	
	protected String getCorpusStagingBatchScratchFilePath(String corpus, String scratchFileName) throws IOException {
		String path = (
				getCorpusPath(corpus) 
				+ "/" 
				+ scratchFileName
		);
		return path;
	}
	
	protected String getCorpusStagingBatchPropertiesPath(String corpus, String stagingBatchName) {
		return (
				getCorpusStagingBatchPath(corpus, stagingBatchName)
				+ "/" 
				+ "BatchProperties.json"
		);
	}
	

	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#storeStagingBatchJson(java.lang.String, java.lang.String, com.fasterxml.jackson.databind.node.ObjectNode)
	 */
	@Override
	public void storeStagingBatchJson(String corpus, String stagingBatchName, ObjectNode properties) throws Exception {
		storeStagingBatchString(corpus, stagingBatchName, getMapper().writeValueAsString(properties));
	}
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#storeStagingBatchString(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void storeStagingBatchString(String corpus, String stagingBatchName, String in) throws Exception {
        Long contentLength = Long.valueOf(in.getBytes("utf-8").length);
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(contentLength);
		metadata.setContentType("application/json");
		metadata.setContentEncoding("utf-8");
		S3Utils.getInstance().getS3Client().putObject(
				new PutObjectRequest(
						getProperties().getProperty("Bucket", defaultBucket),
						getCorpusStagingBatchPropertiesPath(corpus, stagingBatchName),
						IOUtils.toInputStream(in, "UTF-8"),
						metadata
				)
		);		
	}

	@Override
	public ObjectNode listStagingBatches(String corpus) throws Exception {
		ObjectNode json = getMapper().createObjectNode();
		ArrayNode corporaNode = json.putArray("Corpora");

		for (S3ObjectSummary objectSummary: S3Utils.getInstance().getS3Client().listObjects(
				properties.getProperty("Bucket", defaultBucket), 
				getCorpusPath(corpus)
			).getObjectSummaries()) {
			if (objectSummary.getKey().endsWith("/")) {
				corporaNode.add(objectSummary.getKey().substring(0, objectSummary.getKey().length() - 1));
			}
		}
		return json;
	}

	@Override
	public ObjectNode getStagingBatch(String corpus, String stagingBatchName) throws Exception {
		return (ObjectNode) getMapper().readTree(
				S3Utils.getInstance().getS3Client().getObject(
						new GetObjectRequest(
								getProperties().getProperty("Bucket", defaultBucket), 
								getCorpusStagingBatchPropertiesPath(
										corpus, 
										stagingBatchName
								)
						)
				).getObjectContent()
		);
	}
}
