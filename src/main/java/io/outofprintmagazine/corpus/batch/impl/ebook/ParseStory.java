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
package io.outofprintmagazine.corpus.batch.impl.ebook;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(
				new StringReader(
						getTextDocumentFromStorage(inputStepItem)
				)
			);
			boolean seenToc = false;
			boolean inStory = false;
			StringBuffer buf = new StringBuffer();
			String line = null;
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				if (line.equals(inputStepItem.get("esnlc_DocTitleAnnotation").asText())) {
					if (seenToc) {
						inStory = true;
					}
					seenToc = true;
				}
				else if (inStory && inputStepItem.get("nextTitle") != null && inputStepItem.get("nextTitle").asText().length() > 0 && line.equals(inputStepItem.get("nextTitle").asText())) {
					ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
					try {
						setStorageLink(
								getStorage().storeScratchFileString(
									inputStepItem.get("corpusId").asText(), 
									getOutputScratchFilePath(
											inputStepItem.get("stagingLinkStorage").get("objectName").asText() + "_" + inputStepItem.get("esnlc_DocTitleAnnotation").asText(), 
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
													inputStepItem.get("stagingLinkStorage").get("objectName").asText() + "_" + inputStepItem.get("esnlc_DocTitleAnnotation").asText() 
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
					buf.append(line);
					buf.append('\n');
					buf.append('\n');
				}
			}
			if (buf.length() > 0 && inStory) {
				ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
				try {
					setStorageLink(
							getStorage().storeScratchFileString(
								inputStepItem.get("corpusId").asText(), 
								getOutputScratchFilePath(
										inputStepItem.get("stagingLinkStorage").get("objectName").asText() + "_" + inputStepItem.get("esnlc_DocTitleAnnotation").asText(), 
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
												inputStepItem.get("stagingLinkStorage").get("objectName").asText() + "_" + inputStepItem.get("esnlc_DocTitleAnnotation").asText() 
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
		finally {
			if (reader != null) {
				reader.close();
			}
		}
	}
}
