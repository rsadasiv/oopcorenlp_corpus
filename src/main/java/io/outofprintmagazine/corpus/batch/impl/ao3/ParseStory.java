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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;


public class ParseStory extends CorpusBatchStep {
	
	@SuppressWarnings("unused")
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
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);

		String rawHtml = getTextDocumentFromStorage(inputStepItem);
		rawHtml = rawHtml.replace("<br/>", "</p><p>");
		Document doc = Jsoup.parse(
				rawHtml,
				getLink(inputStepItem)
		);
		setAuthor(doc, outputStepItem);
		setDate(doc, outputStepItem);
		setTitle(doc, outputStepItem);
		setThumbnail(getData().getProperties().get("oop_DocThumbnail").asText(), outputStepItem);
		setStorageLink(
				getStorage().storeScratchFileString(
					getData().getCorpusId(), 
					getOutputScratchFilePathFromInput(inputStepItem, "txt"),
					getChapterText(doc).toString().trim()
				),
				outputStepItem
		);
		
		retval.add(outputStepItem);
		return retval;
	}
	
	protected String getAuthor(Document doc) {
		return 
				doc.select(
					getData().getProperties().get("esnlc_AuthorAnnotation").asText()
				).text();
	}
	
	protected String getDate(Document doc) {
		return 
				doc.select(
						getData().getProperties().get("esnlc_DocDateAnnotation").asText()
				).text();
	}
	
	protected String getChapterText(Document document) {
		StringBuffer buf = new StringBuffer();
		Elements chapters = document.select(getData().getProperties().get("chapters").asText());
		for (Element chapter : chapters) {
			buf.append(getText(chapter));
			buf.append("**********");
			buf.append('\n');
			buf.append('\n');
		}
		return buf.toString();
	}
}
