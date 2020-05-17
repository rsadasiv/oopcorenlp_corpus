package io.outofprintmagazine.corpus.batch.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.storage.DocumentStorage;
import io.outofprintmagazine.corpus.storage.mongodb.MongoDBDocumentStorage;

public class MongoDBTextLoader extends CorpusBatchStep {

	private static final Logger logger = LogManager.getLogger(MongoDBTextLoader.class);
	
	private DocumentStorage loader = new MongoDBDocumentStorage();
	
	public MongoDBTextLoader() {
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
		loader.storeAsciiText(
				getData().getCorpusId(),
				getData().getCorpusBatchId(),
				getDocID(inputStepItem),
				getTextDocumentFromStorage(inputStepItem)
		);
		outputStepItem.put("MongoDBTextLoader", "true");
		retval.add(outputStepItem);
		return retval;
	}
}
