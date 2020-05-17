package io.outofprintmagazine.corpus.storage.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.storage.BatchStorage;


public class FileBatchStorage implements BatchStorage {

	protected ObjectMapper getMapper() {
		return mapper; 
	}
	
	protected ObjectMapper mapper;
	
	protected String getDefaultPath() {
		return "C:\\Users\\rsada\\eclipse-workspace\\oopcorenlp_web\\WebContent\\Corpora\\Test";
	}
	
	private Properties properties = new Properties();
	

	public Properties getProperties() {
		return properties;
	}
	
	public FileBatchStorage() {
		super();
		mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		mapper.configure(com.fasterxml.jackson.core.JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
	}
	
	public FileBatchStorage(Properties properties) {
		this();
		properties.putAll(properties);
	}



	@Override
	public void createCorpus(String corpus) throws Exception {
		//pass
	}
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#listCorpora()
	 */
	@Override
	public ObjectNode listCorpora() throws Exception {
		ObjectNode json = getMapper().createObjectNode();
		ArrayNode childNodes = json.putArray("Corpora");
		
		File[] directories = new File(getProperties().getProperty("Path", getDefaultPath())).listFiles(File::isDirectory);		
		for (int i=0;i<directories.length;i++) {
			childNodes.add(directories[i].getName());
		}
		return json;
	}
	
	protected String getCorpusPath(String corpus) {
		String path = (
				getProperties().getProperty("Path", getDefaultPath())
				+ System.getProperty("file.separator", "/")	
				+ corpus
		);
		File dir = new File(path);
		if (!dir.exists()) dir.mkdirs();
		return path;
		
	}
	
	protected String getCorpusStagingBatchPath(String corpus, String stagingBatchName) {
		String path = (
				getCorpusPath(corpus) 
				+ System.getProperty("file.separator", "/") 
				+ stagingBatchName
		);
		File dir = new File(path);
		if (!dir.exists()) dir.mkdirs();
		return path;
	}
	
	protected String getCorpusStagingBatchItemPath(String corpus, String stagingBatchName, String stagingBatchItemName) {
		String path = (
				getCorpusPath(corpus) 
				+ System.getProperty("file.separator", "/") 
				+ stagingBatchName
				+ System.getProperty("file.separator", "/") 
				+ stagingBatchItemName
		);
		File dir = new File(path);
		if (!dir.exists()) dir.mkdirs();
		return path;
	}
	

	protected String getCorpusStagingBatchItemPropertiesPath(String corpus, String stagingBatchName, String stagingBatchItemName) {
		return (
				getCorpusStagingBatchItemPath(corpus, stagingBatchName, stagingBatchItemName)
				+ System.getProperty("file.separator", "/") 
				+ "BatchItemProperties.json"
		);
	}	
	
	protected String getCorpusStagingBatchScratchPath(String corpus, String stagingBatchName) {
		String path = (
				getCorpusStagingBatchPath(corpus, stagingBatchName) 
				+ System.getProperty("file.separator", "/") 
				+ "Scratch"
		);
		File dir = new File(path);
		if (!dir.exists()) dir.mkdirs();
		return path;
	}
	
	protected String getCorpusStagingBatchScratchFilePath(String corpus, String scratchFilePath) throws IOException {
		String path = (
				getCorpusPath(corpus) 
				+ System.getProperty("file.separator", "/") 
				+ scratchFilePath
		);
		File dir = new File(path);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		return path;
	}
	
	protected String getCorpusStagingBatchPropertiesPath(String corpus, String stagingBatchName) {
		return (
				getCorpusStagingBatchPath(corpus, stagingBatchName)
				+ System.getProperty("file.separator", "/") 
				+ "BatchProperties.json"
		);
	}
	
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#storeStagingBatchJson(java.lang.String, java.lang.String, com.fasterxml.jackson.databind.node.ObjectNode)
	 */
	@Override
	public void storeStagingBatchJson(String corpus, String stagingBatchName, ObjectNode properties) throws Exception {
		ObjectWriter writer = getMapper().writer(new DefaultPrettyPrinter());
		writer.writeValue(
				new File(
						getCorpusStagingBatchPropertiesPath(
								corpus, 
								stagingBatchName
						)
				), 
				properties
		);
	}
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#storeStagingBatchString(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void storeStagingBatchString(String corpus, String stagingBatchName, String batchContent) throws Exception {
        File f = new File(getCorpusStagingBatchPropertiesPath(corpus, stagingBatchName));
        FileOutputStream fout = new FileOutputStream(f);
        fout.write(batchContent.getBytes());
        fout.flush();
        fout.close();
	}
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#listStagingBatches(java.lang.String)
	 */
	@Override
	public ObjectNode listStagingBatches(String corpus) throws Exception {
		ObjectNode json = getMapper().createObjectNode();
		ArrayNode childNodes = json.putArray("StagingBatches");
		
		File[] directories = new File(getCorpusPath(corpus)).listFiles(File::isDirectory);		
		for (int i=0;i<directories.length;i++) {
			childNodes.add(directories[i].getName());
		}
		return json;
	}
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#getStagingBatch(java.lang.String, java.lang.String)
	 */
	@Override
	public ObjectNode getStagingBatch(String corpus, String stagingBatchName) throws Exception {
		return (ObjectNode) getMapper().readTree(
				new File(
						getCorpusStagingBatchPropertiesPath(
								corpus, 
								stagingBatchName
						)
				)
		);
	}
	

	public void storeStagingBatchItem(String corpus, String stagingBatchName, String stagingBatchItemName, ObjectNode properties) throws Exception {
		ObjectWriter writer = getMapper().writer(new DefaultPrettyPrinter());
		writer.writeValue(
				new File(
						getCorpusStagingBatchItemPropertiesPath(
								corpus, 
								stagingBatchName,
								stagingBatchItemName
						)
				), 
				properties
		);
	}
	
	
}
