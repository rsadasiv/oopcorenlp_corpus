package io.outofprintmagazine.corpus.storage;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface BatchStorage {

	ObjectNode listCorpora() throws Exception;
	
	void createCorpus(String corpus) throws Exception;

	ObjectNode listStagingBatches(String corpus) throws Exception;

	ObjectNode getStagingBatch(String corpus, String stagingBatchName) throws Exception;
	
	void storeStagingBatchJson(String corpus, String stagingBatchName, ObjectNode properties) throws Exception;

	void storeStagingBatchString(String corpus, String stagingBatchName, String batchContent) throws Exception;
	
}