package io.outofprintmagazine.corpus.batch.impl.gutenberg;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;


public class ParseArchive extends CorpusBatchStep {
	
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(ParseArchive.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
		
	public ParseArchive() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws IOException {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
		setThumbnail(
				String.format(
						getData().getProperties().get("oop_DocThumbnail").asText(),
						inputStepItem.get("ebookNumber").asText(),
						inputStepItem.get("ebookNumber").asText()
				),
				outputStepItem
		);
		setLink( 
				String.format(
						getData().getProperties().get("ebookLink").asText(),
						inputStepItem.get("ebookNumber").asText(),
						inputStepItem.get("ebookNumber").asText(),
						inputStepItem.get("ebookNumber").asText()
				),
				outputStepItem
		);
		retval.add(outputStepItem);
		return retval;
	}
}
