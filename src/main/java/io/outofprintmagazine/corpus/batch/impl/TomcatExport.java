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
package io.outofprintmagazine.corpus.batch.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.batch.ICorpusBatchStep;

public class TomcatExport extends CorpusBatchStep implements ICorpusBatchStep {

	private static final Logger logger = LogManager.getLogger(TomcatExport.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}

	
	public TomcatExport() throws IOException {
		super();
	}
	
	protected String getFilePath(String prefix, String stagingBatchName) {
		return (
				prefix
				+ System.getProperty("file.separator", "/") 
				+ stagingBatchName
				+ System.getProperty("file.separator", "/") 
		);
	}
	
	protected void writeFile(String path, String corpus, String fileName, String content) throws IOException {
		FileOutputStream fout = null;
		try {
			File f = new File(getFilePath(path, corpus) + fileName);
			if (!f.getParentFile().exists()) {
				f.getParentFile().mkdirs();
			}
			fout = new FileOutputStream(f);
			fout.write(content.getBytes());
			fout.flush();
		}
		finally {
			if (fout != null) {
				fout.close();
				fout = null;
			}
		}
	}


	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
		writeFile(
				getData().getProperties().get("webappCorpus_Path").asText(),
				getData().getCorpusId(),
				"TXT_" + getDocID(inputStepItem) + ".txt",
				getTextDocumentFromStorage(
						inputStepItem, 
						"oopNLPTextStorage"
				)
		);
		writeFile(
				getData().getProperties().get("webappCorpus_Path").asText(),
				getData().getCorpusId(),
				"OOP_" + getDocID(inputStepItem) + ".json",
				getTextDocumentFromStorage(
						inputStepItem, 
						"oopNLPStorage"
				)
		);
		writeFile(
				getData().getProperties().get("webappCorpus_Path").asText(),
				getData().getCorpusId(),
				"STANFORD_" + getDocID(inputStepItem) + ".json",
				getTextDocumentFromStorage(
						inputStepItem, 
						"coreNLPStorage"
				)
		);
		writeFile(
				getData().getProperties().get("webappCorpus_Path").asText(),
				getData().getCorpusId(),
				"PIPELINE_" + getDocID(inputStepItem) + ".json",
				getTextDocumentFromStorage(
						inputStepItem, 
						"pipelineStorage"
				)
		);
		if (inputStepItem.has("pollyStorage")) {
			FileOutputStream fout = null;
			try {
				File f = new File(
						getFilePath(
								getData().getProperties().get("webappCorpus_Path").asText(),
								getData().getCorpusId() 
								+ "POLLY_" + getDocID(inputStepItem) + ".mp3"
						)
				);
				fout = new FileOutputStream(f);
				IOUtils.copy(
						getStorage().getScratchFileStream(
								getData().getCorpusId(),
								inputStepItem.get("pollyStorage").asText()
						),
						fout
				);
				fout.flush();
			}
			finally {
				if (fout != null) {
					fout.close();
					fout = null;
				}
			}	
		}
		retval.add(outputStepItem);
		
		return retval;
	}

}
