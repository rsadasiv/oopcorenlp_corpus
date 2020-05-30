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
package io.outofprintmagazine.corpus.batch.impl.wikipedia;

import java.net.URL;
import java.net.URLEncoder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
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
	
	/*
		https://en.wikipedia.org/wiki/List_of_lists_of_lists
	 */
	
	public ParseTOC() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		for (JsonNode range : getData().getProperties().get("range")) {
			JsonNode doc = getMapper().readTree(
					new URL(
							"https://en.wikipedia.org/w/api.php?action=parse&format=json&prop=links&page=" +
									URLEncoder.encode(
											String.format(
													getData().getProperties().get("pageTitle").asText(),
													range.asText()
											),
											"UTF-8"
									)
							)
			);
			//TODO - pagination?
			if (doc.get("parse") != null && doc.get("parse").get("links") != null) {
				JsonNode pagesNode = doc.get("parse").get("links");
				if (pagesNode != null && pagesNode.isArray()) {
					for (JsonNode pageNode : pagesNode) {
						ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
						if (pageNode.get("*") != null ) {
							String pageTitle = pageNode.get("*").asText().replace(' ', '_');
							if (!pageTitle.startsWith("Template") && !pageTitle.startsWith("List")) {
								setLink("https://en.wikipedia.org/wiki/"+pageTitle, outputStepItem);
								retval.add(outputStepItem);
							}
						}
					}
				}
			}
		}
		return retval;		
	}
}
