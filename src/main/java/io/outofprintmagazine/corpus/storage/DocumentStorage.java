package io.outofprintmagazine.corpus.storage;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface DocumentStorage {

	void storeCoreNLP(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception;
	
	ObjectNode getCoreNLP(String corpus, String stagingBatchName, String scratchFileName) throws Exception;
	
	void storeOOPNLP(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception;
	
	ObjectNode getOOPNLP(String corpus, String stagingBatchName, String scratchFileName) throws Exception;
	
	void storeOOPAggregates(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception;
	
	ObjectNode getOOPAggregates(String corpus, String stagingBatchName, String scratchFileName) throws Exception;
	
	public ObjectNode getCorpusAggregateScores(String corpus) throws Exception;
	
	public ObjectNode getCorpusIDFScores(String corpus) throws Exception;
	
	void storeOOPZScores(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception;
	
	void storeOOPTfidfScores(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception;
	
	void storeAsciiText(String corpus, String stagingBatchName, String scratchFileName, String in) throws Exception;
	
	String getAsciiText(String corpus, String stagingBatchName, String scratchFileName) throws Exception;
	
	void storePipelineInfo(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception;
	
	ObjectNode getPipelineInfo(String corpus, String stagingBatchName, String scratchFileName) throws Exception;
	
}