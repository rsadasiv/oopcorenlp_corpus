package io.outofprintmagazine.corpus.batch.impl.ao3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

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
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);

		String rawHtml = getTextDocumentFromStorage(inputStepItem);
		rawHtml = rawHtml.replace("<br/>", "</p><p>");
		Document doc = Jsoup.parse(
				rawHtml,
				getLink(inputStepItem)
		);
		setAuthor(doc, outputStepItem);
		setDate(doc, outputStepItem);
		setTitle(doc, outputStepItem);
		setThumbnail(getData().getProperties().get("oop_DocThumbnail").asText(), outputStepItem);
		setStorageLink(
				getStorage().storeScratchFileString(
					getData().getCorpusId(), 
					getOutputScratchFilePathFromInput(inputStepItem, "txt"),
					getChapterText(doc).toString().trim()
				),
				outputStepItem
		);
		
		retval.add(outputStepItem);
		return retval;
	}
	
	protected String getAuthor(Document doc) {
		return 
				doc.select(
					getData().getProperties().get("esnlc_AuthorAnnotation").asText()
				).text();
	}
	
	protected String getDate(Document doc) {
		return 
				doc.select(
						getData().getProperties().get("esnlc_DocDateAnnotation").asText()
				).text();
	}
	
	protected String getChapterText(Document document) {
		StringBuffer buf = new StringBuffer();
		Elements chapters = document.select(getData().getProperties().get("chapters").asText());
		for (Element chapter : chapters) {
			buf.append(getText(chapter));
			buf.append("**********");
			buf.append('\n');
			buf.append('\n');
		}
		return buf.toString();
	}
}
