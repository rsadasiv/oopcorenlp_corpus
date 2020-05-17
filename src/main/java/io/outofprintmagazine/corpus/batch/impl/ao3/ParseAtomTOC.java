package io.outofprintmagazine.corpus.batch.impl.ao3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;

public class ParseAtomTOC extends CorpusBatchStep {
	
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(ParseAtomTOC.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
			
	public ParseAtomTOC() {
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
						getLink(inputStepItem),
						Parser.xmlParser()
				);
		Elements links = doc.select(getData().getProperties().get("selector").asText());
		for (Element element : links) {
			ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
			setLink(element.select("link").first().attr("href") + "?view_adult=true&view_full_work=true", outputStepItem);
			retval.add(outputStepItem);
		}
		return retval;
	}
}
