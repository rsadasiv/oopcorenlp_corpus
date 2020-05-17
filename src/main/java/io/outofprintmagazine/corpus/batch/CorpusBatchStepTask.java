package io.outofprintmagazine.corpus.batch;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface CorpusBatchStepTask {
	
	public void enrichOne(ObjectNode outputStepItem) throws Exception;

}
