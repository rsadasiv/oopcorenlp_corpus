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
import java.util.Iterator;
import java.util.List;

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

public class DocumentWord2Vec extends CorpusBatchStep implements ICorpusBatchStep {
	
	private static final Logger logger = LogManager.getLogger(DocumentWord2Vec.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}
	
	public DocumentWord2Vec() {
		super();
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
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
		ObjectNode oop = (ObjectNode) getJsonNodeFromStorage(inputStepItem, "oopNLPStorage");
        storeModel(outputStepItem, oop, new NodeToToken(), "Tokens");
        storeModel(outputStepItem, oop, new NodeToLemma(), "Lemmas");
        storeModel(outputStepItem, oop, new NodeToLemma_POS(), "Lemmas_POS");
		retval.add(outputStepItem);
		return retval;
	}
		
	protected void storeModel(ObjectNode outputStepItem, ObjectNode oop, NodeToString serializer, String prefix ) throws Exception {
        Word2Vec vec = new Word2Vec.Builder()
                .minWordFrequency(getData().getProperties().get("minWordFrequency").asInt())
                .iterations(getData().getProperties().get("iterations").asInt())
                .layerSize(getData().getProperties().get("layerSize").asInt())
                .seed(42)
                .windowSize(getData().getProperties().get("windowSize").asInt())
                .iterate(
                		new CollectionSentenceIterator(
                				docToTokenStrings(oop, serializer)
                		)
                )
                .tokenizerFactory(new DefaultTokenizerFactory())
                .build();
        vec.fit();
        File f = File.createTempFile(getDocID(outputStepItem), "word2vec");
        WordVectorSerializer.writeWord2VecModel(vec, f);
        FileInputStream fin = null;
		String storageLocation = null;
		try {
			fin = new FileInputStream(f);
			storageLocation = getStorage().storeScratchFileStream(
				getData().getCorpusId(),
				getOutputScratchFilePath(prefix + "_" + getDocID(outputStepItem), "word2vec"),
				fin
			);
		}
		finally {
			if (fin != null) {
				fin.close();
			}
		}
		if (storageLocation != null) {
			outputStepItem.put(
					"Word2Vec" + prefix + "Storage",
					storageLocation
				);					
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
