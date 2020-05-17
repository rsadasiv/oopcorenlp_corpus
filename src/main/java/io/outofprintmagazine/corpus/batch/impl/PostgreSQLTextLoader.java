package io.outofprintmagazine.corpus.batch.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.storage.DocumentStorage;
import io.outofprintmagazine.corpus.storage.postgresql.PostgreSQLDocumentStorage;

public class PostgreSQLTextLoader extends CorpusBatchStep {

	private static final Logger logger = LogManager.getLogger(PostgreSQLTextLoader.class);
	
	private DocumentStorage loader = new PostgreSQLDocumentStorage();
	
	public PostgreSQLTextLoader() {
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
		outputStepItem.put("PostgreSQLTextLoader", "true");
		retval.add(outputStepItem);
		return retval;
	}
}
