package io.outofprintmagazine.corpus.batch.impl.wikisource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.nodes.Document;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.batch.ICorpusBatchStep;

	
public class ParseStory extends CorpusBatchStep implements ICorpusBatchStep {
		
	private static final Logger logger = LogManager.getLogger(ParseStory.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}
	
	@Override
	public ObjectNode getDefaultProperties() {
		ObjectNode properties = getMapper().createObjectNode();
		properties.put("oop_Text", "div.mw-parser-output p");
		properties.put("esnlc_AuthorAnnotation", "span.gen_header_title #header_author_text");
		properties.put("esnlc_DocTitleAnnotation", "span.gen_header_title #header_section_text");
		return properties;
	}
	
	
	public ParseStory() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		Document doc = getJsoupDocumentFromStorage(inputStepItem);
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
		setTitle(doc, outputStepItem);
		setAuthor(doc, outputStepItem);
		setStorageLink(
				getStorage().storeScratchFileString(
						getData().getCorpusId(), 
						getOutputScratchFilePathFromInput(inputStepItem, "txt"),
						getText(doc).toString().trim()
					),
				outputStepItem
		);
		
		retval.add(outputStepItem);
		return retval;
	}
	
}
