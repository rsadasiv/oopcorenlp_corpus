package io.outofprintmagazine.corpus.storage;

import java.io.InputStream;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface CorpusStorage {

	ObjectNode listCorpora() throws Exception;
	
	void createCorpus(String corpus) throws Exception;

	ObjectNode listStagingBatches(String corpus) throws Exception;

	ObjectNode getStagingBatch(String corpus, String stagingBatchName) throws Exception;
	
	void storeStagingBatchJson(String corpus, String stagingBatchName, ObjectNode properties) throws Exception;

	void storeStagingBatchString(String corpus, String stagingBatchName, String batchContent) throws Exception;
	
	String getFileNameFromPath(String scratchFilePath);
	
	String trimFileExtension(String scratchFileName);
	
	String getScratchFilePath(String stagingBatchName, String stagingBatchStepName, String scratchFileName) throws Exception;
	
	ObjectNode storeScratchFileString(String corpus, String scratchFilePath, ObjectNode properties, String in) throws Exception;

	ObjectNode storeScratchFileStream(String corpus, String scratchFilePath, ObjectNode properties, InputStream in) throws Exception;

	String getScratchFileString(String corpus, String scratchFilePath) throws Exception;
	
	InputStream getScratchFileStream(String corpus, String scratchFilePath) throws Exception;

	ObjectNode getScratchFileProperties(String corpus, String scratchFilePath) throws Exception;

	InputStream getScratchFilePropertiesStream(String corpus, String scratchFilePath) throws Exception;
	
	ObjectNode storeCoreNLP(String corpus, String stagingBatchName, String scratchFileName, ObjectNode properties, ObjectNode in) throws Exception;
	
	ObjectNode getCoreNLP(String corpus, String stagingBatchName, String scratchFileName) throws Exception;
	
	ObjectNode storeOOPNLP(String corpus, String stagingBatchName, String scratchFileName, ObjectNode properties, ObjectNode in) throws Exception;
	
	ObjectNode getOOPNLP(String corpus, String stagingBatchName, String scratchFileName) throws Exception;
	
	ObjectNode storeAsciiText(String corpus, String stagingBatchName, String scratchFileName, ObjectNode properties, String in) throws Exception;
	
	String getAsciiText(String corpus, String stagingBatchName, String scratchFileName) throws Exception;
	
	ObjectNode storePipelineInfo(String corpus, String stagingBatchName, String scratchFileName, ObjectNode properties, ObjectNode in) throws Exception;
	
	ObjectNode getPipelineInfo(String corpus, String stagingBatchName, String scratchFileName) throws Exception;

}