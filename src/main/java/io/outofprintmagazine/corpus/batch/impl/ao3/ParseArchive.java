/*******************************************************************************
 * Copyright (C) 2020 Ram Sadasiv
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
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
