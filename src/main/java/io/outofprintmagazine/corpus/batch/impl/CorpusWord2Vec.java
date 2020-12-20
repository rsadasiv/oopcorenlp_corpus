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
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.CollectionSentenceIterator;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.batch.ICorpusBatchStep;

public class CorpusWord2Vec extends CorpusBatchStep implements ICorpusBatchStep {
	
	private static final Logger logger = LogManager.getLogger(CorpusWord2Vec.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}
	
	private Map<String, Collection<String>> sentenceCollections = new HashMap<String, Collection<String>>();
	
	protected Map<String, Collection<String>> getSentenceCollections() {
		return sentenceCollections;
	}
	
	protected Collection<String> getSentenceCollection(String name) {
		return getSentenceCollections().get(name);
	}
	
	public CorpusWord2Vec() {
		super();
		getSentenceCollections().put("Tokens", new ArrayList<String>());
		getSentenceCollections().put("Lemmas", new ArrayList<String>());
		getSentenceCollections().put("Lemmas_POS", new ArrayList<String>());
	}
	
	@Override
	public ObjectNode getDefaultProperties() {
		ObjectNode properties = getMapper().createObjectNode();
		properties.put("minWordFrequency", 2);
		properties.put("iterations", 100);
		properties.put("layerSize", 50);
		properties.put("windowSize", 5);
		return properties;
	}
	
	@Override
	public ArrayNode run(ArrayNode input)  {
		ArrayNode retval = super.run(input);
		try {
			storeModel("Tokens");
			storeModel("Lemmas");
			storeModel("Lemmas_POS");
		}
		catch (Exception e) {
			e.printStackTrace();
			getLogger().error(e);
		}
		return retval;
	}
	

	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
		ObjectNode oop = (ObjectNode) getJsonNodeFromStorage(inputStepItem, "oopNLPStorage");
		getSentenceCollection("Tokens").addAll(docToTokenStrings(oop, new NodeToToken()));
		getSentenceCollection("Lemmas").addAll(docToTokenStrings(oop, new NodeToLemma()));
		getSentenceCollection("Lemmas_POS").addAll(docToTokenStrings(oop, new NodeToLemma_POS()));
		outputStepItem.put(
				"oopNLPCorpusAggregatesWord2Vec_TokensStorage",
				getStorage().getScratchFilePath(
						getData().getCorpusBatchId(), 
						getData().getCorpusBatchStepId(),
						"CORPUS_AGGREGATES_WORD2VEC_Tokens.word2vec" 
				)
		);
		outputStepItem.put(
				"oopNLPCorpusAggregatesWord2Vec_LemmasStorage",
				getStorage().getScratchFilePath(
						getData().getCorpusBatchId(), 
						getData().getCorpusBatchStepId(),
						"CORPUS_AGGREGATES_WORD2VEC_Lemmas.word2vec" 
				)
		);
		outputStepItem.put(
				"oopNLPCorpusAggregatesWord2Vec_Lemmas_POSStorage",
				getStorage().getScratchFilePath(
						getData().getCorpusBatchId(), 
						getData().getCorpusBatchStepId(),
						"CORPUS_AGGREGATES_WORD2VEC_Lemmas_POS.word2vec" 
				)
		);		
		retval.add(outputStepItem);
		return retval;
	}
		
	protected void storeModel(String prefix) throws Exception {
        Word2Vec vec = new Word2Vec.Builder()
                .minWordFrequency(getData().getProperties().get("minWordFrequency").asInt())
                .iterations(getData().getProperties().get("iterations").asInt())
                .layerSize(getData().getProperties().get("layerSize").asInt())
                .seed(42)
                .windowSize(getData().getProperties().get("windowSize").asInt())
                .iterate(
                		new CollectionSentenceIterator(
                				getSentenceCollection(prefix)
                		)
                )
                .tokenizerFactory(new DefaultTokenizerFactory())
                .build();
        vec.fit();
        File f = File.createTempFile(prefix, "word2vec");
        WordVectorSerializer.writeWord2VecModel(vec, f);
        FileInputStream fin = null;
		try {
			fin = new FileInputStream(f);
			getStorage().storeScratchFileStream(
				getData().getCorpusId(),
				getOutputScratchFilePath("CORPUS_AGGREGATES_WORD2VEC_" + prefix, "word2vec"),
				fin
			);
		}
		finally {
			if (fin != null) {
				fin.close();
			}
		}		
	}

	abstract class NodeToString {
		abstract String nodeToString(ObjectNode node);
	}
	
	class NodeToToken extends NodeToString {
		public String nodeToString(ObjectNode node) {
			return node.get("TokensAnnotation").get("word").asText();
		}
	}
	
	class NodeToLemma extends NodeToString {
		public String nodeToString(ObjectNode node) {
			return node.get("TokensAnnotation").get("lemma").asText();
		}
	}
	
	class NodeToLemma_POS extends NodeToString {
		public String nodeToString(ObjectNode node) {
			return node.get("TokensAnnotation").get("lemma").asText() + "_" + node.get("TokensAnnotation").get("pos").asText();
		}
	}
	
	protected List<String> docToTokenStrings(ObjectNode doc, NodeToString serializer) {
        ArrayNode sentences = (ArrayNode) doc.get("sentences");
        List<String> cleanedSentences = new ArrayList<String>();
        Iterator<JsonNode> sentencesIter = sentences.iterator();
        while (sentencesIter.hasNext()) {
        	JsonNode sentenceNode = sentencesIter.next();
        	StringBuffer buf = new StringBuffer();
        	ArrayNode tokensNode = (ArrayNode) sentenceNode.get("tokens");
        	Iterator<JsonNode> tokensIter = tokensNode.iterator();
        	while (tokensIter.hasNext()) {
        		ObjectNode tokenNode = (ObjectNode) tokensIter.next();
        		//keep all the verbs
        		if (
        				tokenNode.has("OOPActionlessVerbsAnnotation")
        				|| tokenNode.has("OOPVerbsAnnotation")
        			) {
        			buf.append(serializer.nodeToString(tokenNode));
        			buf.append(" ");
        		}
        		else if (
        				!tokenNode.get("TokensAnnotation").get("pos").asText().equals("POS")
        				&& !tokenNode.get("TokensAnnotation").get("pos").asText().equals("NFP")
        				&& !tokenNode.has("OOPPunctuationMarkAnnotation")
        				//&& !stopWords.contains(tokenNode.get("TokensAnnotation").get("lemma").asText().toLowerCase())
        				//&& !tokenNode.has("OOPCommonWordsAnnotation") 
        				&& !tokenNode.has("OOPFunctionWordsAnnotation")
        			) {
        			buf.append(serializer.nodeToString(tokenNode));
        			buf.append(" ");
        		}
        	}
        	
        	cleanedSentences.add(buf.toString());
        }
        return cleanedSentences;
	}

}
