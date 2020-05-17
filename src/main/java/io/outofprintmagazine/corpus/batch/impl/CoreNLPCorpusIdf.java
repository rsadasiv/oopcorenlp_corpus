package io.outofprintmagazine.corpus.batch.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.storage.DocumentStorage;
import io.outofprintmagazine.corpus.storage.postgresql.PostgreSQLDocumentStorage;

public class CoreNLPCorpusIdf extends CorpusBatchStep {
	
	private static final Logger logger = LogManager.getLogger(CoreNLPCorpusIdf.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
	
	private DocumentStorage loader = new PostgreSQLDocumentStorage();
	private String scratchFilePath = null;
	
	public CoreNLPCorpusIdf() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode input) throws Exception {
		if (scratchFilePath == null) {
			scratchFilePath = getStorage().storeScratchFileObject(
					getData().getCorpusId(),
					getOutputScratchFilePath("CorpusIdfScores", "json"),
					loader.getCorpusIDFScores(getData().getCorpusId())
			);
		}
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(input);
		outputStepItem.put(
				"oopNLPCorpusIdfScoresStorage",
				scratchFilePath
		);
		
		retval.add(outputStepItem);
		return retval;
	}
		
	@Override
	public void configure(ObjectNode properties) {
		getData().setProperties(properties);
	}

}
