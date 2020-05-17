package io.outofprintmagazine.corpus.batch.impl.dna;

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
		
	/*
	 * TOPICS
	 * 
	 * death
	 * environment
	 * literature
	 * marriage
	 * mental-illness
	 * violence
	 */
	
	/*
	 * SECTIONS
	 * 
	 * business
	 * entertainment
	 * india
	 * lifestyle
	 * technology
	 */
	
	public ParseArchive() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws IOException {
		ArrayNode retval = getMapper().createArrayNode();
		for (int i=1;i<inputStepItem.get("pageCount").asInt(10)+1;i++) {
			ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
			setLink(
					String.format(
							getData().getProperties().get("link").asText(),
							inputStepItem.get("archive").asText(),
							i
					),	
					outputStepItem
			);
			retval.add(outputStepItem);
		}
		return retval;
	}
}
