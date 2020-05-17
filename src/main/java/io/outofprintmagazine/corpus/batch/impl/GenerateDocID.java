package io.outofprintmagazine.corpus.batch.impl;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;

public class GenerateDocID extends CorpusBatchStep {

	public GenerateDocID() {
		super();
	}

	private static final Logger logger = LogManager.getLogger(GenerateDocID.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}

	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode output = copyInputToOutput(inputStepItem);
		setDocID(
				output, 
				DigestUtils.md5Hex(
						getTextDocumentFromStorage(
								inputStepItem
						)
				)
		);
		retval.add(output);
		return retval;
	}

}
