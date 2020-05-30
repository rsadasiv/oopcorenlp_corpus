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
package io.outofprintmagazine.corpus.batch.impl.oop;

import java.io.IOException;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;


public class ParseStory extends CorpusBatchStep {
	
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
		Document doc = getJsoupDocumentFromStorage(inputStepItem);
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
	
		StringBuffer textBuf = new StringBuffer();
		Elements titles = doc.select("h5");
		for (Element title : titles) {
			if (title.childNode(0).nodeName() == "a") {
				Node aNode = title.childNode(0);
				if (aNode.childNodes().size() > 0 && aNode.childNode(0).nodeName() == "strong") {
					Node strongNode = aNode.childNode(0);
					if (strongNode.childNodes().size() > 0 && strongNode.childNode(0).nodeName() == "span") {
						setTitle(strongNode.childNode(0).childNode(0).outerHtml().trim(), outputStepItem);
						String authorName = title.select("strong").get(0).textNodes().get(0).getWholeText().trim().substring("by ".length()).trim();
						setAuthor(authorName.split(",")[0].trim(), outputStepItem);
					}
				}
			}
			if (title.childNode(0).nodeName() == "span") {
				setTitle(title.childNode(0).childNode(0).outerHtml().trim(), outputStepItem);

				if (title.textNodes().get(0).getWholeText().trim().contains("by ")) {
					String authorName = title.textNodes().get(0).getWholeText().trim().substring("by ".length()).trim();
					setAuthor(authorName.split(",")[0].trim(), outputStepItem);
				}
			}
			
		}
		Elements mainText = doc.select("div#main-text-cont2");
		for (Element body : mainText ) {
			Elements paragraphs = body.select("p");
			for (Element paragraph: paragraphs) {
				if (!paragraph.attr("class").startsWith("writersintro")) {
					//story.setBody(story.getBody() + '\n' + paragraph.text());
					textBuf.append(paragraph.wholeText().trim());
					textBuf.append('\n');
					//fucktard
					//textBuf.append('\n');
				}
			}
		}

		setStorageLink(
				getStorage().storeScratchFileString(
					inputStepItem.get("corpusId").asText(), 
					getOutputScratchFilePathFromInput(inputStepItem, "txt"),
					textBuf.toString().trim()
				), 
				outputStepItem
		);
		retval.add(outputStepItem);
		return retval;
	}
}
