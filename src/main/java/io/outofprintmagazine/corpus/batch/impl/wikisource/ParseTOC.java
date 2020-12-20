package io.outofprintmagazine.corpus.batch.impl.wikisource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.batch.ICorpusBatchStep;


public class ParseTOC extends CorpusBatchStep implements ICorpusBatchStep {
	
	private static final Logger logger = LogManager.getLogger(ParseTOC.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}
	
	public ParseTOC() {
		super();
	}
	
	@Override
	public ObjectNode getDefaultProperties() {
		ObjectNode properties = getMapper().createObjectNode();
		properties.put("esnlc_AuthorAnnotation", "div.gen_header_title #header_author_text");
		properties.put("esnlc_DocDateAnnotation", "div.gen_header_title");
		properties.put("esnlc_DocTitleAnnotation", "div.gen_header_title #header_title_text");
		properties.put("selector", "div.mw-parser-output a");
		return properties;
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		Document doc = getJsoupDocumentFromStorage(inputStepItem);
		
		Elements links = doc.select(getData().getProperties().get("selector").asText());
		for (Element element : links) {
			if (
					element.hasAttr("href")
					&& element.attr("href").startsWith("/wiki")
					&& !element.attr("href").startsWith("/wiki/Author")
					&& !element.attr("href").startsWith("/wiki/Special")
				) {
				ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
				setAuthor(doc, outputStepItem);
				setDate(doc, outputStepItem);
				outputStepItem.remove("stagingLinkStorage");
				setLink("https://en.wikisource.org" + element.attr("href"), outputStepItem);

				retval.add(outputStepItem);
			}
		}
		return retval;
	}
}
