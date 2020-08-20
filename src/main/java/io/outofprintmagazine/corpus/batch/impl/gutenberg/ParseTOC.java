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
		properties.put("selector", "p.toc");
		return properties;
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
