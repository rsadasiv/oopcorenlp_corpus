package io.outofprintmagazine.corpus.batch.impl.wikipedia;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.nodes.Document;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;


public class ParseStory extends CorpusBatchStep {
	
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(ParseStory.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
	
	
	public ParseStory() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		Document doc = getJsoupDocumentFromStorage(inputStepItem);
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
		setAuthor("Wikipedians", outputStepItem);
		setDate(outputStepItem);
		setTitle(doc, outputStepItem);
		setThumbnail(doc, outputStepItem);
		setStorageLink(
				getStorage().storeScratchFileString(
					getData().getCorpusId(), 
					getOutputScratchFilePathFromInput(inputStepItem, "txt"),
					getText(doc).toString().trim().replaceAll("\\[[0-9]{1,2}\\]", "")
				),
				outputStepItem
		);
		
		retval.add(outputStepItem);
		return retval;
	}
}
