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

import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;


public class ParseBodyStory extends CorpusBatchStep {
	
	private static final Logger logger = LogManager.getLogger(ParseBodyStory.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}
		
	public ParseBodyStory() {
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
		boolean inStory = false;
		StringBuffer buf = new StringBuffer();
		Elements paragraphs = doc.selectFirst(
			getData().getProperties().get("selector").asText()
		).children();

		for (Element paragraph : paragraphs) {
			if (paragraph.selectFirst(getText(inputStepItem)) != null) {
				inStory = true;
			}	
			else if (inStory && inputStepItem.get("oop_TextNext").asText().length() > 0 && paragraph.selectFirst(inputStepItem.get("oop_TextNext").asText()) != null) {
				try {
					setStorageLink(
							getStorage().storeScratchFileString(
								getData().getCorpusId(), 
								getOutputScratchFilePath(
										getStorageLink(inputStepItem) + "_" + getTitle(inputStepItem), 
										"txt"
								),
								buf.toString().trim()
							),
							outputStepItem
					);
				}
				catch (IOException ioe) {
					setStorageLink(
							getStorage().storeScratchFileString(
								inputStepItem.get("corpusId").asText(), 
								getOutputScratchFilePath(
										DigestUtils.md5Hex(
												getStorageLink(inputStepItem) + "_" + getTitle(inputStepItem) 
										),
										"txt"
								),
								buf.toString().trim()
							),
							outputStepItem
					);
				}
				retval.add(outputStepItem);
				inStory = false;
				break;
			}
			else if (inStory) {
				if (paragraph.tagName().equalsIgnoreCase("p")) {
					buf.append(paragraph.text().trim());
					buf.append('\n');
					buf.append('\n');
				}
			}
		}


		if (buf.length() > 0 && inStory) {
			try {
				setStorageLink(
						getStorage().storeScratchFileString(
							inputStepItem.get("corpusId").asText(), 
							getOutputScratchFilePath(
									getStorageLink(inputStepItem) + "_" + getTitle(inputStepItem), 
									"txt"
							),
							buf.toString().trim()
						),
						outputStepItem
				);
			}
			catch (IOException ioe) {
				setStorageLink(
						getStorage().storeScratchFileString(
							inputStepItem.get("corpusId").asText(), 
							getOutputScratchFilePath(
									DigestUtils.md5Hex(
											getStorageLink(inputStepItem) + "_" + getTitle(inputStepItem) 
									),
									"txt"
							),
							buf.toString().trim()
						),
						outputStepItem
				);
			}
			retval.add(outputStepItem);
		}
		return retval;
	}
}
