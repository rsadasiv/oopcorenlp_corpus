package io.outofprintmagazine.corpus.batch.impl.gutenberg;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
		Document doc = getJsoupDocumentFromStorage(inputStepItem);
		Elements links = doc.select(getData().getProperties().get("selector").asText());
		Element lastElement = null;
		for (Element element : links) {
			if (element.selectFirst("a") != null) {
				if (lastElement != null) {
					ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
					setTitle(lastElement.wholeText().trim(), outputStepItem);
					outputStepItem.put(
						"oop_Text", 
						"a[name="+lastElement.selectFirst("a").attr("href").substring(1)+"]"
					);
					outputStepItem.put(
						"oop_TextNext", 
						"a[name="+element.selectFirst("a").attr("href").substring(1)+"]"
					);
					retval.add(outputStepItem);
				}
				lastElement = element;
			}
		}
		if (lastElement != null) {
			ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
			setTitle(lastElement.wholeText().trim(), outputStepItem);
			outputStepItem.put(
				"oop_Text", 
				"a[name="+lastElement.selectFirst("a").attr("href").substring(1)+"]"
			);
			outputStepItem.put(
					"oop_TextNext", 
					""
				);
			retval.add(outputStepItem);
		}

		return retval;
	}
	
}
