package io.outofprintmagazine.corpus.storage.s3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.storage.CorpusStorage;

public abstract class S3Corpora implements CorpusStorage {

	//extends FileCorpora
	//Path=//
	//Bucket=

	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(S3Corpora.class);
	
	protected Logger getLogger() {
		return logger;
	}
	
	private String defaultBucket = "oop-corpora";
	
	protected ObjectMapper getMapper() {
		return mapper; 
	}
	
	protected ObjectMapper mapper;
	
	protected String getDefaultPath() {
		return "Test";
	}
	private Properties properties = new Properties();
	
	public Properties getProperties() {
		return properties;
	}
	
	public S3Corpora() {
		super();
		mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		mapper.configure(com.fasterxml.jackson.core.JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
	}
	
	public S3Corpora(Properties properties) {
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
	
	@Override
	public String getScratchFilePath(String stagingBatchName, String stagingBatchItemName, String scratchFileName) throws Exception {
		return (
				stagingBatchName
				+ "/"
				+ stagingBatchItemName
				+ "/"
				+ scratchFileName
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


	public void storeStagingBatchItem(String corpus, String stagingBatchName, String stagingBatchItemName, ObjectNode properties) throws Exception {
		ObjectWriter writer = getMapper().writer(new DefaultPrettyPrinter());
		String buf = writer.writeValueAsString(properties);
		Long contentLength = Long.valueOf(buf.getBytes("utf-8").length);
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(contentLength);
		metadata.setContentType("application/json");
		metadata.setContentEncoding("utf-8");
		S3Utils.getInstance().getS3Client().putObject(
				new PutObjectRequest(
						getProperties().getProperty("Bucket", defaultBucket),
						getCorpusStagingBatchItemPropertiesPath(
								corpus, 
								stagingBatchName,
								stagingBatchItemName
						),
						IOUtils.toInputStream(buf, "UTF-8"),
						metadata
				)
		);
	}

	@Override
	public ObjectNode storeScratchFileString(String corpus, String scratchFileName, ObjectNode properties, String in) throws Exception {
		return storeScratchFileObject(corpus, scratchFileName, properties, in);
		//storeScratchFileProperties(corpus, stagingBatchName, scratchFileName, properties);
	}
	
	protected ObjectNode storeScratchFileObject(String corpus, String scratchFileName, ObjectNode properties, String in) throws Exception {
        Long contentLength = Long.valueOf(in.getBytes("utf-8").length);
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(contentLength);
		metadata.setContentType(properties.get("mimeType").asText("application/json"));
		metadata.setContentEncoding("utf-8");
		S3Utils.getInstance().getS3Client().putObject(
				new PutObjectRequest(
						getProperties().getProperty("Bucket", defaultBucket),
						getCorpusStagingBatchScratchFilePath(
								corpus, 
								scratchFileName
						),
						IOUtils.toInputStream(in, "UTF-8"),
						metadata
				)
		);
		ObjectNode storageProperties = getMapper().createObjectNode();
        storageProperties.put("corpusId", corpus);
        storageProperties.put("storageEngine", "S3");
        storageProperties.put(
        		"objectName",
				getCorpusStagingBatchScratchFilePath(
						corpus, 
						scratchFileName
				)
        );
        return storageProperties;
	}

	protected ObjectNode storeScratchFileProperties(String corpus, String stagingBatchName, String scratchFileName, ObjectNode properties) throws Exception {
		ObjectWriter writer = getMapper().writer(new DefaultPrettyPrinter());
		String buf = writer.writeValueAsString(properties);

        Long contentLength = Long.valueOf(buf.getBytes("utf-8").length);
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(contentLength);
		metadata.setContentType("application/json");
		metadata.setContentEncoding("utf-8");
		S3Utils.getInstance().getS3Client().putObject(
				new PutObjectRequest(
						getProperties().getProperty("Bucket", defaultBucket),
						getCorpusStagingBatchScratchFilePath(
								corpus, 
								scratchFileName +".properties"
						),
						IOUtils.toInputStream(buf, "UTF-8"),
						metadata
				)
		);
		ObjectNode storageProperties = getMapper().createObjectNode();
        storageProperties.put("corpusId", corpus);
        storageProperties.put("corpusBatchId", stagingBatchName);
        storageProperties.put("storageEngine", "S3");
        storageProperties.put(
        		"objectName",
				getCorpusStagingBatchScratchFilePath(
						corpus, 
						scratchFileName
				)
        );
        return storageProperties;
	}

	@Override
	public ObjectNode storeScratchFileStream(String corpus, String scratchFileName, ObjectNode properties, InputStream in) throws Exception {
		//do I need to buffer this in a file?
        File f = File.createTempFile(scratchFileName, "docx");
        FileOutputStream fout = new FileOutputStream(f);
        IOUtils.copy(in,fout);
        in.close();
        fout.flush();
        fout.close();

		S3Utils.getInstance().getS3Client().putObject(
				new PutObjectRequest(
						getProperties().getProperty("Bucket", defaultBucket),
						getCorpusStagingBatchScratchFilePath(
								corpus, 
								scratchFileName
						),
						f
				)
		);
		ObjectNode storageProperties = getMapper().createObjectNode();
        storageProperties.put("corpusId", corpus);
        storageProperties.put("storageEngine", "S3");
        storageProperties.put(
        		"objectName",
				getCorpusStagingBatchScratchFilePath(
						corpus, 
						scratchFileName
				)
        );
        return storageProperties;
		//return storeScratchFileObject(corpus, stagingBatchName, scratchFileName, properties, s);
		//return storeScratchFileProperties(corpus, stagingBatchName, scratchFileName, properties);
	}
		
	
	
	public ObjectNode storeJsonFile(String corpus, String scratchFileName, ObjectNode properties, ObjectNode in) throws Exception {
		ObjectWriter writer = getMapper().writer(new DefaultPrettyPrinter());
		String buf = writer.writeValueAsString(in);
		return storeScratchFileObject(corpus, scratchFileName, properties, buf);
		//return storeScratchFileProperties(corpus, stagingBatchName, scratchFileName, properties);
	}

	
	public ObjectNode storeJsonFileStream(String corpus, String scratchFileName, ObjectNode properties, InputStream in) throws Exception {
		return storeScratchFileObject(corpus, scratchFileName, properties, IOUtils.toString(in, "utf-8"));
		//return storeScratchFileProperties(corpus, stagingBatchName, scratchFileName, properties);
	}

	@Override
	public InputStream getScratchFileStream(String corpus, String scratchFileName) throws Exception {
		return S3Utils.getInstance().getS3Client().getObject(
				new GetObjectRequest(
						getProperties().getProperty("Bucket", defaultBucket), 
						scratchFileName
				)
		).getObjectContent();

	}

	@Override
	public String getScratchFileString(String corpus, String scratchFileName) throws Exception {
		return IOUtils.toString(
				S3Utils.getInstance().getS3Client().getObject(
						new GetObjectRequest(
								getProperties().getProperty("Bucket", defaultBucket), 
								scratchFileName
						)
				).getObjectContent(),
				"utf-8"
		);
	}

	@Override
	public InputStream getScratchFilePropertiesStream(String corpus,  String scratchFileName) throws Exception {
		return getScratchFileStream(corpus, scratchFileName + ".properties");
	}
	
	@Override
	public ObjectNode getScratchFileProperties(String corpus,  String scratchFileName) throws Exception {
		return (ObjectNode) getMapper().readTree(getScratchFileStream(corpus, scratchFileName + ".properties"));
	}

	@Override
	public ObjectNode storeCoreNLP(String corpus, String stagingBatchName, String scratchFileName,
			ObjectNode properties, ObjectNode in) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectNode getCoreNLP(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectNode storeOOPNLP(String corpus, String stagingBatchName, String scratchFileName, ObjectNode properties,
			ObjectNode in) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectNode getOOPNLP(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectNode storeAsciiText(String corpus, String stagingBatchName, String scratchFileName,
			ObjectNode properties, String in) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getAsciiText(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectNode storePipelineInfo(String corpus, String stagingBatchName, String scratchFileName,
			ObjectNode properties, ObjectNode in) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectNode getPipelineInfo(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String getFileNameFromPath(String scratchFilePath) {
		String[] paths = scratchFilePath.split("/");
		return paths[paths.length-1];
	}
	
	public String trimFileExtension(String scratchFileName) {
		int idx = scratchFileName.lastIndexOf(".");
		if (idx < 1) {
			idx = scratchFileName.length();
		}   
		return scratchFileName.substring(0, idx);
	}
}
