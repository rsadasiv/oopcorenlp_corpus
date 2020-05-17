package io.outofprintmagazine.corpus.batch.impl.oop;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

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
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		Document doc = getJsoupDocumentFromStorage(inputStepItem);
		Elements links = doc.select(getData().getProperties().get("selector").asText("a[href^=archive]"));
		for (Element link : links) {
			String ref = link.parent().childNode(0).attr("href").trim();
			if (ref.length()>0 && ref.startsWith("archive")) {
				ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
				outputStepItem.remove("stagingLinkStorage");
				setLink("https://www.outofprintmagazine.co.in/" + ref, outputStepItem);
				outputStepItem.put("issueDate", ref.substring("archive/".length(), ref.length()-"_issue/index.html".length()));
				outputStepItem.put("issueBaseHref", "https://www.outofprintmagazine.co.in/" + ref.substring(0, ref.length()-"index.html".length()));
				retval.add(outputStepItem);
			}
		}
		return retval;
	}
}
