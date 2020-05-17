package io.outofprintmagazine.corpus.storage.s3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.storage.ScratchStorage;

public class S3ScratchStorage implements ScratchStorage {

	//extends FileCorpora
	//Path=//
	//Bucket=

	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(S3ScratchStorage.class);
	
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
	
	public S3ScratchStorage() {
		super();
		mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		mapper.configure(com.fasterxml.jackson.core.JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
	}
	
	public S3ScratchStorage(Properties properties) {
		this();
		properties.putAll(properties);
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


	@Override
	public String storeScratchFileString(String corpus, String scratchFileName, String in) throws Exception {
		return storeScratchFileObject(corpus, scratchFileName, in);
		//storeScratchFileProperties(corpus, stagingBatchName, scratchFileName, properties);
	}
	
	protected String storeScratchFileObject(String corpus, String scratchFileName, String in) throws Exception {
        Long contentLength = Long.valueOf(in.getBytes("utf-8").length);
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(contentLength);
		//metadata.setContentType(properties.get("mimeType").asText("application/json"));
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
		return scratchFileName;

	}

	//TODO - Plain text?
	@Override
	public String storeScratchFileStream(String corpus, String scratchFileName, InputStream in) throws Exception {
		//do I need to buffer this in a file?
		File f = null;
		FileOutputStream fout = null;
		
		try {
			f = File.createTempFile(scratchFileName, scratchFileName.substring(trimFileExtension(scratchFileName).length()+1));
			fout = new FileOutputStream(f);
			IOUtils.copy(in,fout);
			in.close();
			fout.flush();

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
		}
		finally {
			if (fout != null) {
				fout.close();
				fout = null;
			}
			if (f != null) {
				f.delete();
				f = null;
			}
		}
		return scratchFileName;
		
	}
	
	@Override
	public String storeScratchFileObject(String corpus, String scratchFilePath, ObjectNode in) throws Exception {
		return storeJsonFile(corpus, scratchFilePath, in);
	}
		
	
	
	public String storeJsonFile(String corpus, String scratchFileName, ObjectNode in) throws Exception {
		ObjectWriter writer = getMapper().writer(new DefaultPrettyPrinter());
		String buf = writer.writeValueAsString(in);
		return storeScratchFileObject(corpus, scratchFileName, buf);
		//return storeScratchFileProperties(corpus, stagingBatchName, scratchFileName, properties);
	}

	
	public String storeJsonFileStream(String corpus, String scratchFileName, InputStream in) throws Exception {
		return storeScratchFileObject(corpus, scratchFileName, IOUtils.toString(in, "utf-8"));
		//return storeScratchFileProperties(corpus, stagingBatchName, scratchFileName, properties);
	}

	@Override
	public InputStream getScratchFileStream(String corpus, String scratchFileName) throws Exception {
		return S3Utils.getInstance().getS3Client().getObject(
				new GetObjectRequest(
						getProperties().getProperty("Bucket", defaultBucket), 
						getCorpusStagingBatchScratchFilePath(corpus, scratchFileName)
				)
		).getObjectContent();

	}

	@Override
	public String getScratchFileString(String corpus, String scratchFileName) throws Exception {
		getLogger().debug(String.format("s3:get %s %s", corpus, scratchFileName));
		return IOUtils.toString(
				S3Utils.getInstance().getS3Client().getObject(
						new GetObjectRequest(
								getProperties().getProperty("Bucket", defaultBucket), 
								getCorpusStagingBatchScratchFilePath(corpus, scratchFileName)
						)
				).getObjectContent(),
				"utf-8"
		);
	}
	
	@Override
	public ObjectNode getScratchFileProperties(String corpus,  String scratchFileName) throws Exception {
		return null;
	}

	@Override
	public String getFileNameFromPath(String scratchFilePath) {
		String[] paths = scratchFilePath.split(Pattern.quote("/"));
		return paths[paths.length-1];
	}
	
	@Override
	public String trimFileExtension(String scratchFileName) {
		int idx = scratchFileName.lastIndexOf(".");
		if (idx < 1) {
			idx = scratchFileName.length();
		}   
		return scratchFileName.substring(0, idx);
	}


}
