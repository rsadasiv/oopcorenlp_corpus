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
package io.outofprintmagazine.corpus.batch.impl.dropbox;

import java.io.BufferedReader;
import java.io.StringReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;

public class CleanStory extends CorpusBatchStep {
		
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(CleanStory.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
		
	public CleanStory() {
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
			boolean inStory = false;
	
			StringBuffer buf = new StringBuffer();
			String line = null;
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				if (!inStory && (!inputStepItem.has("esnlc_DocTitleAnnotation") || inputStepItem.get("esnlc_DocTitleAnnotation").asText().trim().equals(""))) {
					if (line.length() > 5 && !line.toLowerCase().startsWith("by")) {
						inputStepItem.put("esnlc_DocTitleAnnotation", line);
						continue;
					}
				}
				if (!inStory && (!inputStepItem.has("esnlc_AuthorAnnotation") || inputStepItem.get("esnlc_AuthorAnnotation").asText().trim().equals(""))) {
					if (line.length() > 5 && line.toLowerCase().startsWith("by")) {
						inputStepItem.put("esnlc_AuthorAnnotation", line.substring("by".length()).trim());
						continue;
					}
				}
				if (!inStory && inputStepItem.has("esnlc_DocTitleAnnotation") && line.contains(inputStepItem.get("esnlc_DocTitleAnnotation").asText().trim())) {
					continue;
				}
				if (!inStory && inputStepItem.has("esnlc_AuthorAnnotation") && line.contains(inputStepItem.get("esnlc_AuthorAnnotation").asText().trim())) {
					continue;
				}
				if (
						line.toLowerCase().startsWith("email: ") 
						|| line.toLowerCase().startsWith("from: ") 
						|| line.toLowerCase().startsWith("to: ") 
						|| line.toLowerCase().startsWith("subject: ")
						|| line.toLowerCase().startsWith("word count: ")
						|| line.toLowerCase().startsWith("bio: ")
						|| line.toLowerCase().startsWith("genre: ")
						|| line.toLowerCase().startsWith("translated ")
				) {
					continue;
				}
				if (!inStory && line.length() > 25) {
					inStory = true;
				}
				if (inStory) {
					buf.append(line);
					buf.append('\n');
					buf.append('\n');
				}
			}
			if (buf.length() > 0 && inStory) {
				ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
				setStorageLink(
						getStorage().storeScratchFileString(
							getData().getCorpusId(),
							getOutputScratchFilePathFromInput(inputStepItem, "txt"),
							buf.toString().trim()
						),
						outputStepItem
				);
				retval.add(outputStepItem);
			}
			return retval;
		}
		catch (Exception e) {
			getLogger().error(e);
			throw e;
		}
		finally {
			if (reader != null) {
				reader.close();
			}
		}
	}
}
