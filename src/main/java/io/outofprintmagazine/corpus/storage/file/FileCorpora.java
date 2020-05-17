package io.outofprintmagazine.corpus.storage.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.storage.CorpusStorage;


public abstract class FileCorpora implements CorpusStorage {
	//implements CorpusStorage {

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
	
	public FileCorpora() {
		super();
		mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		mapper.configure(com.fasterxml.jackson.core.JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
	}
	
	public FileCorpora(Properties properties) {
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
	
	@Override
	public String getScratchFilePath(String stagingBatchName, String stagingBatchItemName, String scratchFileName) throws Exception {
		return (
				stagingBatchName
				+ System.getProperty("file.separator", "/") 
				+ stagingBatchItemName
				+ System.getProperty("file.separator", "/") 
				+ scratchFileName
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
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#storeScratchFile(java.lang.String, java.lang.String, java.lang.String, com.fasterxml.jackson.databind.node.ObjectNode, java.lang.String)
	 */
	@Override
	public ObjectNode storeScratchFileString(String corpus, String scratchFilePath, ObjectNode properties, String in) throws Exception {
		ObjectNode storageProperties = getMapper().createObjectNode();
        File f = new File(getCorpusStagingBatchScratchFilePath(corpus, scratchFilePath));
        FileOutputStream fout = new FileOutputStream(f);
        fout.write(in.getBytes());
        fout.flush();
        fout.close();
        storageProperties.put("storageEngine", this.getClass().getName());
        storageProperties.put("objectName", getCorpusStagingBatchScratchFilePath(corpus, scratchFilePath));
        f = new File(getCorpusStagingBatchScratchFilePath(corpus, scratchFilePath) + ".properties");
        getMapper().writeValue(f, properties);
        return storageProperties;
	}
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#storeScratchFileStream(java.lang.String, java.lang.String, java.lang.String, com.fasterxml.jackson.databind.node.ObjectNode, java.io.InputStream)
	 */
	@Override
	public ObjectNode storeScratchFileStream(String corpus, String scratchFilePath, ObjectNode properties, InputStream in) throws Exception {
		ObjectNode storageProperties = getMapper().createObjectNode();
        File f = new File(getCorpusStagingBatchScratchFilePath(corpus,  scratchFilePath));
        FileOutputStream fout = new FileOutputStream(f);
        IOUtils.copy(in,fout);
        in.close();
        fout.flush();
        fout.close();
        storageProperties.put("corpusId", corpus);
        storageProperties.put("storageEngine", "file");
        storageProperties.put("objectName", f.getName());
        f = new File(getCorpusStagingBatchScratchFilePath(corpus, scratchFilePath) + ".properties");
        getMapper().writeValue(f, properties);
        return storageProperties;
	}
	

	public ObjectNode storeJsonFile(String corpus, String scratchFileName, ObjectNode properties, ObjectNode in) throws Exception {
		ObjectNode storageProperties = getMapper().createObjectNode();
        File f = new File(getCorpusStagingBatchScratchFilePath(corpus, scratchFileName));
        getMapper().writeValue(f, in);
        storageProperties.put("corpusId", corpus);
        storageProperties.put("storageEngine", "file");
        storageProperties.put("objectName", f.getName());
        f = new File(getCorpusStagingBatchScratchFilePath(corpus, scratchFileName) + ".properties");
        getMapper().writeValue(f, properties);
        return storageProperties;
	}
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#storeScratchFileStream(java.lang.String, java.lang.String, java.lang.String, com.fasterxml.jackson.databind.node.ObjectNode, java.io.InputStream)
	 */

	public ObjectNode storeJsonFileStream(String corpus, String scratchFileName, ObjectNode properties, InputStream in) throws Exception {
		ObjectNode storageProperties = getMapper().createObjectNode();
        File f = new File(getCorpusStagingBatchScratchFilePath(corpus, scratchFileName));
        FileOutputStream fout = new FileOutputStream(f);
        IOUtils.copy(in,fout);
        in.close();
        fout.flush();
        fout.close();
        storageProperties.put("corpusId", corpus);
        storageProperties.put("storageEngine", "file");
        storageProperties.put("objectName", f.getName());
        f = new File(getCorpusStagingBatchScratchFilePath(corpus, scratchFileName) + ".properties");
        getMapper().writeValue(f, properties);
        return storageProperties;
	}
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#getScratchFileStream(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public InputStream getScratchFileStream(String corpus, String scratchFileName) throws Exception {
        File f = new File(getCorpusStagingBatchScratchFilePath(corpus, scratchFileName));
        FileInputStream fin = new FileInputStream(f);
        return fin;
	}
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#getScratchFileString(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public String getScratchFileString(String corpus, String scratchFileName) throws Exception {	
	    return IOUtils.toString(
	    		getScratchFileStream(corpus, scratchFileName),
	    		"utf-8"
	    );
	}
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#getScratchFilePropertiesStream(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public InputStream getScratchFilePropertiesStream(String corpus, String scratchFileName) throws Exception {
        File f = new File(getCorpusStagingBatchScratchFilePath(corpus, scratchFileName + ".properties"));
        FileInputStream fin = new FileInputStream(f);
        return fin;
	}
	
	@Override
	public ObjectNode getScratchFileProperties(String corpus, String scratchFileName) throws Exception {
		return (ObjectNode) getMapper().readTree(getScratchFilePropertiesStream(corpus, scratchFileName));
	}

	@Override
	public ObjectNode storeCoreNLP(String corpus, String stagingBatchName, String scratchFileName,
			ObjectNode properties, ObjectNode in) throws Exception {
		return storeJsonFile(corpus, scratchFileName, properties, in);
	}

	@Override
	public ObjectNode storeOOPNLP(String corpus, String stagingBatchName, String scratchFileName, ObjectNode properties,
			ObjectNode in) throws Exception {
		return storeJsonFile(corpus, scratchFileName, properties, in);
	}

	@Override
	public ObjectNode storeAsciiText(String corpus, String stagingBatchName, String scratchFileName,
			ObjectNode properties, String in) throws Exception {
		return storeScratchFileString(corpus, scratchFileName, properties, in);
	}

	@Override
	public ObjectNode storePipelineInfo(String corpus, String stagingBatchName, String scratchFileName,
			ObjectNode properties, ObjectNode in) throws Exception {
		return storeJsonFile(corpus, scratchFileName, properties, in);
	}


	@Override
	public ObjectNode getCoreNLP(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		return (ObjectNode) getMapper().readTree(getScratchFileStream(corpus, scratchFileName));
	}

	@Override
	public ObjectNode getOOPNLP(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		return (ObjectNode) getMapper().readTree(getScratchFileStream(corpus, scratchFileName));
	}

	@Override
	public String getAsciiText(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		return getScratchFileString(corpus, scratchFileName);
	}

	@Override
	public ObjectNode getPipelineInfo(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		return (ObjectNode) getMapper().readTree(getScratchFileStream(corpus, scratchFileName));
	}

	public String getFileNameFromPath(String scratchFilePath) {
		String[] paths = scratchFilePath.split(System.getProperty("file.separator", "/"));
		return paths[paths.length-1];
	}
	
	public String trimFileExtension(String scratchFileName) {
		int idx = scratchFileName.lastIndexOf(".");
		if (idx < 1) {
			idx = scratchFileName.length();
		}   
		return scratchFileName.substring(0, idx-1);
	}

}
