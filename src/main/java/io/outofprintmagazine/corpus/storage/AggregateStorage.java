package io.outofprintmagazine.corpus.storage;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface AggregateStorage {
	
	//ObjectNode storeCorpusAggregate(String corpus, String stagingBatchName, String scratchFileName, ObjectNode properties, ObjectNode in) throws Exception;
	
	ObjectNode getCorpusAggregateScoreStats(String corpus) throws Exception;
	
	ObjectNode getCorpusAggregateSubScoreStats(String corpus, String score) throws Exception;

}