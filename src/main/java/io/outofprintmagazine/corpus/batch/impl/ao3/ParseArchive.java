package io.outofprintmagazine.corpus.batch.impl.ao3;

import java.io.IOException;
import java.net.URLEncoder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

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
	public ArrayNode runOne(ObjectNode inputStepItem) throws IOException {
		ArrayNode retval = getMapper().createArrayNode();
		String nextPage = String.format(
				getData().getProperties().get("feedLink").asText(),
				URLEncoder.encode(inputStepItem.get("tag").asText(), "utf-8").replace("+", "%20").replace(".", "*d*")
		);
		while (nextPage != null) {
			ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
			setLink(nextPage, outputStepItem);
			retval.add(outputStepItem);
			nextPage = hasNext(nextPage);
		}
		return retval;
	}
	
	private String hasNext(String url) throws IOException {
		Document doc = Jsoup.connect(url).get();
		Element next = doc.selectFirst(getData().getProperties().get("selector").asText());
		if (next != null && next.hasAttr("href")) {
			return getData().getProperties().get("baseHref").asText() + next.attr("href");
		}
		else {
			return null;
		}	
	}
}
