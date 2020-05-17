package io.outofprintmagazine.corpus.batch.impl.word2vec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.batch.CorpusBatchStepTask;

public class NerSentencePreprocessor extends CorpusBatchStep implements CorpusBatchStepTask {

	private static final Logger logger = LogManager.getLogger(NerSentencePreprocessor.class);
		
	public NerSentencePreprocessor() {
		super();
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}

	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		return getMapper().createArrayNode();	}
	
	@Override
	public void enrichOne(ObjectNode outputStepItem) throws Exception {
		ObjectNode in = (ObjectNode) getJsonNodeFromStorage(outputStepItem, "oopNLPStorage");
		StringBuffer txt = new StringBuffer();
		int sentenceIdx = 0;
		for (JsonNode sentence : in.get("sentences")) {
			if (sentence.get("SentenceIndexAnnotation").asInt(1) > sentenceIdx) {
				txt.append("\n");
				sentenceIdx = sentence.get("SentenceIndexAnnotation").asInt(1);
			}
			for (JsonNode token : sentence.get("tokens")) {
				if (
						!token.has("OOPFunctionWordsAnnotation") 
						&& isDictionaryWord(token.get("TokensAnnotation").get("pos").asText("X"))
				) {
					if (token.get("TokensAnnotation").has("ner")  && !token.get("TokensAnnotation").get("ner").asText("O").equals("O")) {
						txt.append(token.get("TokensAnnotation").get("ner").asText("O"));
        				txt.append(" ");
					}
					else {
						txt.append(token.get("TokensAnnotation").get("lemma").asText());
						txt.append(" ");
					}
				}
			}
		}
		outputStepItem.put(
				"word2vec_NerSentence_Preprocessed",
				getStorage().storeScratchFileString(
					getData().getCorpusId(),
					getOutputScratchFilePath("NerSentence_" + getDocID(outputStepItem), "txt"),
					txt.toString()
			)
		);
	}
}
