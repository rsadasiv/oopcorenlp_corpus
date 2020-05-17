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

public class ParseTOC extends CorpusBatchStep {
	
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(ParseTOC.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
		
	public ParseTOC() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		Document doc = 
				Jsoup.parse(
						getStorage().getScratchFileStream(
								getData().getCorpusId(),
								getStorageLink(inputStepItem)
						),
						"utf-8",
						getLink(inputStepItem)
				);
		Elements links = doc.select(getData().getProperties().get("selector").asText());
		for (Element element : links) {
			if (!element.hasAttr("rel")) {
				ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
				setLink(
						getData().getProperties().get("baseHref").asText() + element.attr("href") + "?view_adult=true&view_full_work=true",
						outputStepItem
				);
				retval.add(outputStepItem);
			}
		}
		return retval;
	}
}
