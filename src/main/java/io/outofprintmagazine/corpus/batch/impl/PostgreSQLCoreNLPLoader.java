package io.outofprintmagazine.corpus.batch.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.storage.DocumentStorage;
import io.outofprintmagazine.corpus.storage.postgresql.PostgreSQLDocumentStorage;

public class PostgreSQLCoreNLPLoader extends CorpusBatchStep {

	private static final Logger logger = LogManager.getLogger(PostgreSQLCoreNLPLoader.class);
	
	private DocumentStorage loader = new PostgreSQLDocumentStorage();
	
	public PostgreSQLCoreNLPLoader() {
		super();
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}

	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);

		loader.storeCoreNLP(
				getData().getCorpusId(),
				getData().getCorpusBatchId(),
				getDocID(inputStepItem),
				(ObjectNode) getJsonNodeFromStorage(inputStepItem, "coreNLPStorage")
		);
		
		loader.storePipelineInfo(
				getData().getCorpusId(),
				getData().getCorpusBatchId(),
				getDocID(inputStepItem),
				(ObjectNode) getJsonNodeFromStorage(inputStepItem, "pipelineStorage")
		);

		loader.storeOOPNLP(
				getData().getCorpusId(),
				getData().getCorpusBatchId(),
				getDocID(inputStepItem),
				(ObjectNode) getJsonNodeFromStorage(inputStepItem, "oopNLPStorage")
		);
		
		loader.storeOOPAggregates(
				getData().getCorpusId(),
				getData().getCorpusBatchId(),
				getDocID(inputStepItem),
				(ObjectNode) getJsonNodeFromStorage(inputStepItem, "oopNLPAggregatesStorage")
		);
		

		outputStepItem.put("PostgreSQLCoreNLPLoader", "true");
		retval.add(outputStepItem);
		return retval;
	}
	
}
