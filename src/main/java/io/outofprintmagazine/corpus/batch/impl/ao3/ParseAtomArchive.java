package io.outofprintmagazine.corpus.batch.impl.ao3;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;


public class ParseAtomArchive extends CorpusBatchStep {
	
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(ParseAtomArchive.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
	
	public ParseAtomArchive() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws IOException {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);

		setLink(
			String.format(
					getData().getProperties().get("feedLink").asText(),
					inputStepItem.get("tag").asText()
			),
			outputStepItem
		);
		retval.add(outputStepItem);
		return retval;
	}
}
