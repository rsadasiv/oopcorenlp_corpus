package io.outofprintmagazine.corpus.storage.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.storage.ScratchStorage;


public class FileScratchStorage implements ScratchStorage {
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
	
	public FileScratchStorage() {
		super();
		mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		mapper.configure(com.fasterxml.jackson.core.JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
	}
	
	public FileScratchStorage(Properties properties) {
		this();
		properties.putAll(properties);
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
		File file = new File(path);
		if (!file.getParentFile().exists()) {
			file.getParentFile().mkdirs();
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
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#storeScratchFile(java.lang.String, java.lang.String, java.lang.String, com.fasterxml.jackson.databind.node.ObjectNode, java.lang.String)
	 */
	//TODO try/finally
	@Override
	public String storeScratchFileString(String corpus, String scratchFilePath, String in) throws Exception {
        File f = new File(getCorpusStagingBatchScratchFilePath(corpus, scratchFilePath));
        FileOutputStream fout = new FileOutputStream(f);
        fout.write(in.getBytes());
        fout.flush();
        fout.close();
        return scratchFilePath;
	}
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#storeScratchFileStream(java.lang.String, java.lang.String, java.lang.String, com.fasterxml.jackson.databind.node.ObjectNode, java.io.InputStream)
	 */
	//TODO try/finally
	@Override
	public String storeScratchFileStream(String corpus, String scratchFilePath, InputStream in) throws Exception {
        File f = new File(getCorpusStagingBatchScratchFilePath(corpus,  scratchFilePath));
        FileOutputStream fout = new FileOutputStream(f);
        IOUtils.copy(in,fout);
        in.close();
        fout.flush();
        fout.close();
        return scratchFilePath;

	}
	
	//TODO try/finally
	@Override
	public String storeScratchFileObject(String corpus, String scratchFilePath, ObjectNode in) throws Exception {
		return storeJsonFile(corpus, scratchFilePath, in);
	}
	

	public String storeJsonFile(String corpus, String scratchFilePath, ObjectNode in) throws Exception {
        File f = new File(getCorpusStagingBatchScratchFilePath(corpus, scratchFilePath));
        getMapper().writeValue(f, in);
        return scratchFilePath;
	}
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#storeScratchFileStream(java.lang.String, java.lang.String, java.lang.String, com.fasterxml.jackson.databind.node.ObjectNode, java.io.InputStream)
	 */
	
	public String storeJsonFileStream(String corpus, String scratchFileName, InputStream in) throws Exception {
        File f = new File(getCorpusStagingBatchScratchFilePath(corpus, scratchFileName));
        FileOutputStream fout = new FileOutputStream(f);
        IOUtils.copy(in,fout);
        in.close();
        fout.flush();
        fout.close();
        return scratchFileName;
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
	
	@Override
	public ObjectNode getScratchFileProperties(String corpus, String scratchFileName) throws Exception {
		return null;
	}


	@Override
	public String getFileNameFromPath(String scratchFilePath) {
		String[] paths = scratchFilePath.split(Pattern.quote(System.getProperty("file.separator", "/")));
		return paths[paths.length-1];
	}
	
	@Override
	public String trimFileExtension(String scratchFileName) {
		int idx = scratchFileName.lastIndexOf(".");
		if (idx < 1) {
			idx = scratchFileName.length();
		}   
		return scratchFileName.substring(0, idx-1);
	}



}
