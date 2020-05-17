package io.outofprintmagazine.corpus.batch.impl.dna;

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
		Document doc = Jsoup.parse(
				getStorage().getScratchFileStream(
						getData().getCorpusId(),
						getStorageLink(inputStepItem)
				), 
				"utf-8",
				inputStepItem.get("link").asText()
		);
		Elements links = doc.select(getData().getProperties().get("selector").asText());
		for (Element element : links) {
			ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
			outputStepItem.remove("stagingLinkStorage");
			setLink("https://www.dnaindia.com" + element.attr("href"), outputStepItem);
			retval.add(outputStepItem);
		}
		return retval;
	}
}
