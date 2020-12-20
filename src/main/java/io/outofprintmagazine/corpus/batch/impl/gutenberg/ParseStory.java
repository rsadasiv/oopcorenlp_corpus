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
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.batch.ICorpusBatchStep;


public class ParseStory extends CorpusBatchStep implements ICorpusBatchStep {
	
	private static final Logger logger = LogManager.getLogger(ParseStory.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}
		
	public ParseStory() {
		super();
	}
	
	@Override
	public ObjectNode getDefaultProperties() {
		ObjectNode properties = getMapper().createObjectNode();
		properties.put("selector", "p");
		return properties;
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
		Document doc = getJsoupDocumentFromStorageNormalized(inputStepItem);
		boolean inStory = false;
		StringBuffer buf = new StringBuffer();
		Elements paragraphs = doc.select(
			getData().getProperties().get("selector").asText()
		);

		for (Element paragraph : paragraphs) {
			if (paragraph.selectFirst(inputStepItem.get("oop_Text").asText()) != null) {
				inStory = true;
			}	
			else if (inStory && inputStepItem.get("oop_TextNext").asText().length() > 0 && paragraph.selectFirst(inputStepItem.get("oop_TextNext").asText()) != null) {
				try {
					setStorageLink(
							getStorage().storeScratchFileString(
								getData().getCorpusId(), 
								getOutputScratchFilePath(
										getTitle(inputStepItem), 
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
								getData().getCorpusId(), 
								getOutputScratchFilePath(
										DigestUtils.md5Hex(
												getTitle(inputStepItem) 
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
				buf.append(Parser.unescapeEntities(paragraph.text().trim(), false));
				buf.append('\n');
				buf.append('\n');
			}
		}

		if (buf.length() > 0 && inStory) {
			try {
				setStorageLink(
						getStorage().storeScratchFileString(
							getData().getCorpusId(), 
							getOutputScratchFilePath(
									getTitle(inputStepItem), 
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
							getData().getCorpusId(), 
							getOutputScratchFilePath(
									DigestUtils.md5Hex(
											getTitle(inputStepItem) 
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
