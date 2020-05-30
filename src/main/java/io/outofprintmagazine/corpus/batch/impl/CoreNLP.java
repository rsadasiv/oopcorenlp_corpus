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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.stanford.nlp.ling.CoreAnnotations;
import io.outofprintmagazine.Analyzer;
import io.outofprintmagazine.corpus.batch.CorpusBatchStep;

public class CoreNLP extends CorpusBatchStep {
	
	private static final Logger logger = LogManager.getLogger(CoreNLP.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
	
	private Analyzer ta = null;
	
	public CoreNLP() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		if (ta == null) {
			List<String> customAnnotators = new ArrayList<>();
			Iterator<JsonNode> customAnnotatorsIter = ((ArrayNode) getData().getProperties().get("customAnnotators")).elements();
			while (customAnnotatorsIter.hasNext()){
			    customAnnotators.add(customAnnotatorsIter.next().asText());
			}
			ta = new Analyzer(getParameterStore(), customAnnotators);
		}
		ArrayNode retval = getMapper().createArrayNode();
		try {
			String doc = getTextDocumentFromStorage(inputStepItem);
			
			ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
//			ObjectNode storageProperties = getMapper().createObjectNode();
//			//"Sun, 16 Feb 2020 23:17:38 GMT"
//			storageProperties.put("Content-Type", "text/plain");
//			storageProperties.put("mimeType", "text/plain");
//			storageProperties.put("charset",  "us-ascii");
//			storageProperties.put("Date", getDateFormat().format(new Date(System.currentTimeMillis())));
	
			Properties metadata = new Properties();
			metadata.put(CoreAnnotations.DocIDAnnotation.class.getName(), getDocID(inputStepItem));
			metadata.put(CoreAnnotations.DocTypeAnnotation.class.getName(), getData().getCorpusId());
			metadata.put(CoreAnnotations.AuthorAnnotation.class.getName(), getAuthor(inputStepItem));
			metadata.put(CoreAnnotations.DocDateAnnotation.class.getName(), getDate(inputStepItem));
			metadata.put(CoreAnnotations.DocTitleAnnotation.class.getName(), getTitle(inputStepItem));
			metadata.put(CoreAnnotations.DocSourceTypeAnnotation.class.getName(), getLink(inputStepItem));
			
			//this should read the list of custom annotators from the properties
			Map<String,ObjectNode> json = ta.analyze(metadata, doc);
						
			outputStepItem.put(
				"coreNLPStorage",
				getStorage().storeScratchFileObject(
					getData().getCorpusId(),
					getOutputScratchFilePath("STANFORD_" + getDocID(inputStepItem), "json"),
					json.get("STANFORD")
				)
			);
			
			outputStepItem.put(
				"oopNLPStorage",
				getStorage().storeScratchFileObject(
					getData().getCorpusId(),
					getOutputScratchFilePath("OOP_" + getDocID(inputStepItem), "json"),
					json.get("OOP")
				)
			);
			
			outputStepItem.put(
					"oopNLPAggregatesStorage",
					getStorage().storeScratchFileObject(
						getData().getCorpusId(),
						getOutputScratchFilePath("AGGREGATES_" + getDocID(inputStepItem), "json"),
						json.get("AGGREGATES")
				)
			);
			
			outputStepItem.put(
				"pipelineStorage",
				getStorage().storeScratchFileObject(
					getData().getCorpusId(),
					getOutputScratchFilePath("PIPELINE_" + getDocID(inputStepItem), "json"),
					json.get("PIPELINE")
				)
			);
	
			retval.add(outputStepItem);
		}
		catch (ClassNotFoundException e) {
			getLogger().error(e);
			throw new IOException(e);
		} 
		catch (InstantiationException e) {
			getLogger().error(e);
			throw new IOException(e);
		} 
		catch (IllegalAccessException e) {
			getLogger().error(e);
			throw new IOException(e);
		}
		return retval;
	}
	
	@Override
	public void configure(ObjectNode properties) {
		getData().setProperties(properties);
	}

}
