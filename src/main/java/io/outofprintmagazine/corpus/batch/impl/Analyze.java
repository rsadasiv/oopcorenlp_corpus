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
import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.batch.ICorpusBatchStep;
import io.outofprintmagazine.nlp.Analyzer;
import io.outofprintmagazine.nlp.pipeline.OOPAnnotations.OOPThumbnailAnnotation;

public class Analyze extends CorpusBatchStep implements ICorpusBatchStep {
	
	private static final Logger logger = LogManager.getLogger(Analyze.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}
	
	@Override
	public ObjectNode getDefaultProperties() {
		ObjectNode properties = getMapper().createObjectNode();
		ArrayNode customAnnotators = getMapper().createArrayNode();
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.BiberAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.CoreNlpParagraphAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.CoreNlpGenderAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.GenderAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.PronounAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.count.CharCountAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.count.ParagraphCountAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.count.SentenceCountAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.count.SyllableCountAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.count.TokenCountAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.count.WordCountAnnotator");	    		
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.CoreNlpSentimentAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.VaderSentimentAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.VerbTenseAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.PunctuationMarkAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.AdjectivesAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.PointlessAdjectivesAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.AdjectiveCategoriesAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.AdverbsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.PointlessAdverbsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.AdverbCategoriesAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.PossessivesAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.PrepositionCategoriesAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.PrepositionsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.VerbsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.ActionlessVerbsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.NounsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.TopicsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.SVOAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.NonAffirmativeAnnotator");	    		
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.simile.LikeAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.simile.AsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.ColorsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.FlavorsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.VerblessSentencesAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.WordlessWordsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.WordnetGlossAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.PerfecttenseAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.UncommonWordsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.CommonWordsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.FunctionWordsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.AngliciseAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.AmericanizeAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.VerbGroupsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.VerbnetGroupsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.NounGroupsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.TemporalNGramsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.interrogative.WhoAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.interrogative.WhatAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.interrogative.WhenAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.interrogative.WhereAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.interrogative.WhyAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.interrogative.HowAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.LocationsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.PeopleAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.MyersBriggsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.BiberDimensionsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.DatesAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.conditional.IfAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.conditional.BecauseAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.QuotesAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.WordsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.FleschKincaidAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.VerbHypernymsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.NounHypernymsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.WikipediaGlossAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.WikipediaPageviewTopicsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.WikipediaCategoriesAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.ActorsAnnotator");
		customAnnotators.add("io.outofprintmagazine.nlp.pipeline.annotators.SettingsAnnotator");
		properties.set("customAnnotators", customAnnotators);
		return properties;
	}
	
	private Analyzer ta = null;
	
	public Analyze() {
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
			metadata.put(CoreAnnotations.DocIDAnnotation.class.getSimpleName(), getDocID(inputStepItem));
			metadata.put(CoreAnnotations.DocTypeAnnotation.class.getSimpleName(), getData().getCorpusId());
			metadata.put(CoreAnnotations.AuthorAnnotation.class.getSimpleName(), getAuthor(inputStepItem));
			metadata.put(CoreAnnotations.DocDateAnnotation.class.getSimpleName(), getDate(inputStepItem));
			metadata.put(CoreAnnotations.DocTitleAnnotation.class.getSimpleName(), getTitle(inputStepItem));
			metadata.put(CoreAnnotations.DocSourceTypeAnnotation.class.getSimpleName(), getLink(inputStepItem));
			if (inputStepItem.has("oop_DocThumbnail")) {
				metadata.put(OOPThumbnailAnnotation.class.getSimpleName(), inputStepItem.get("oop_DocThumbnail").asText("blank.png"));
			}
			//this should read the list of custom annotators from the properties
			Map<String,ObjectNode> json = ta.analyze(metadata, doc);

			outputStepItem.put(
					"oopNLPTextStorage",
					getStorage().storeScratchFileString(
						getData().getCorpusId(),
						getOutputScratchFilePath("TEXT_" + getDocID(inputStepItem), "txt"),
						doc
					)
				);
			
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

}
