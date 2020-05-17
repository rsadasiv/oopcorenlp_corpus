package io.outofprintmagazine.corpus.batch.impl.word2vec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.batch.CorpusBatchStepTask;

public class LemmaPosParagraphPreprocessor extends CorpusBatchStep implements CorpusBatchStepTask {

	private static final Logger logger = LogManager.getLogger(LemmaPosParagraphPreprocessor.class);
		
	public LemmaPosParagraphPreprocessor() {
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
		int paragraphIdx = 0;
		for (JsonNode sentence : in.get("sentences")) {
			if (sentence.get("ParagraphIndexAnnotation").asInt(1) > paragraphIdx) {
				txt.append("\n");
				paragraphIdx = sentence.get("ParagraphIndexAnnotation").asInt(1);
			}
			for (JsonNode token : sentence.get("tokens")) {
				if (
						!token.has("OOPFunctionWordsAnnotation") 
						&& isDictionaryWord(token.get("TokensAnnotation").get("pos").asText("X"))
				) {
					txt.append(token.get("TokensAnnotation").get("lemma").asText());
					txt.append("_");
					txt.append(token.get("TokensAnnotation").get("pos").asText());
					txt.append(" ");
				}
			}
		}
		outputStepItem.put(
				"word2vec_LemmaPosParagraph_Preprocessed",
				getStorage().storeScratchFileString(
					getData().getCorpusId(),
					getOutputScratchFilePath("LemmaPosParagraph_" + getDocID(outputStepItem), "txt"),
					txt.toString()
			)
		);

	}
	


}
