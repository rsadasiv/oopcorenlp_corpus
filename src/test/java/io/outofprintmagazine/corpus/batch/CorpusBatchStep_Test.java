package io.outofprintmagazine.corpus.batch;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.model.CorpusBatchStepModel;
import io.outofprintmagazine.corpus.storage.IScratchStorage;
import io.outofprintmagazine.corpus.storage.file.FileScratchStorage;
import io.outofprintmagazine.util.IParameterStore;
import io.outofprintmagazine.util.ParameterStoreLocal;

public class CorpusBatchStep_Test {

	public CorpusBatchStep_Test() throws IOException {
		super();
		
	}
	
	@BeforeAll
	public static void init() {
		try {
			FileUtils.deleteDirectory(new File("../Corpora_Test"));
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	/**
	 * 	#http://wordnetcode.princeton.edu/wn3.1.dict.tar.gz
	 *  wordNet_location=dict
	 *  #http://verbs.colorado.edu/verb-index/vn/verbnet-3.3.tar.gz
	 *  verbNet_location=verbnet3.3/
     *
	 *	#https://www.mediawiki.org/wiki/API:Etiquette
	 *	wikipedia_apikey=OOPCoreNlp/0.9.1 httpclient/4.5.6
	*/
	private IParameterStore parameterStore = null;

	public IParameterStore getParameterStore() throws IOException {
		if (parameterStore == null) {
			ObjectMapper mapper = new ObjectMapper();
			ObjectNode p = mapper.createObjectNode();
			p.put("wordNet_location", "../data/dict");
			p.put("verbNet_location", "../data/verbnet3.3");
			p.put("wikipedia_apikey", "OOPCoreNlp/0.9.1 httpclient/4.5.6");
			p.put("fileCorpus_Path", "../Corpora_Test");
			parameterStore = new ParameterStoreLocal(p);
		}
		return parameterStore;
	}
	
	private IScratchStorage storage = null;
	
	public IScratchStorage getStorage() throws IOException {
		if (storage == null) {
			storage = new FileScratchStorage();
			storage.setParameterStore(getParameterStore());
		}
		return storage;
	}
	
	
	public void initCorpusBatchStep(ICorpusBatchStep currentBatchStep, CorpusBatchStepModel corpusBatchStepModel) throws IOException {

		currentBatchStep.setData(corpusBatchStepModel);
		currentBatchStep.setStorage(getStorage());
		currentBatchStep.setParameterStore(getParameterStore());
		ObjectMapper mapper = new ObjectMapper();
		if (corpusBatchStepModel.getInput() == null) {
			corpusBatchStepModel.setInput(mapper.createArrayNode());
		}
		if (corpusBatchStepModel.getOutput() == null) {
			corpusBatchStepModel.setOutput(mapper.createArrayNode());
		}
		
	}

	
	public void checkStoryMetadata(JsonNode outputNode) throws Exception {
		assertTrue(
				outputNode.hasNonNull("esnlc_AuthorAnnotation") 
				&& outputNode.get("esnlc_AuthorAnnotation").asText().length() > 0, 
				"missing esnlc_AuthorAnnotation"
		);
		assertTrue(
				outputNode.has("esnlc_DocDateAnnotation") 
				&& outputNode.get("esnlc_DocDateAnnotation").asText().length() > 0, 
				"missing esnlc_DocDateAnnotation"
		);
		assertTrue(
				outputNode.has("esnlc_DocTitleAnnotation") 
				&& outputNode.get("esnlc_DocTitleAnnotation").asText().length() > 0, 
				"missing esnlc_DocTitleAnnotation"
		);		
	}
	

	
	@Test
	public void cleanText_Test() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		ICorpusBatchStep currentBatchStep = new io.outofprintmagazine.corpus.batch.impl.CleanText();
		
		CorpusBatchStepModel corpusBatchStepModel = new CorpusBatchStepModel();
		corpusBatchStepModel.setCorpusId("Test");
		corpusBatchStepModel.setCorpusBatchId("Batch");
		corpusBatchStepModel.setCorpusBatchStepId("CleanText");
		corpusBatchStepModel.setCorpusBatchStepSequenceId(new Integer(0));

		String uncleanText = "\u201CNow is the time for all good men to come to the aid of their party.\u201D";
		String cleanText = "\"Now is the time for all good men to come to the aid of their party.\"";

		ObjectNode inputNode = mapper.createObjectNode();

		inputNode.put(
				"stagingLinkStorage",
				getStorage().storeScratchFileString(
						corpusBatchStepModel.getCorpusId(), 
						getStorage().getScratchFilePath(
								corpusBatchStepModel.getCorpusBatchId(),
								"UncleanText",
								"UncleanText.txt"),
						uncleanText
				)
		);
		initCorpusBatchStep(currentBatchStep, corpusBatchStepModel);
		
		ArrayNode outputNodes = currentBatchStep.runOne(inputNode);
		assertTrue(outputNodes.size() > 0, "runOne returned nothing");		
		for (JsonNode outputNode : outputNodes) {
			String txt = getStorage().getScratchFileString(
					corpusBatchStepModel.getCorpusId(),
					outputNode.get("stagingLinkStorage").asText()
			);
			assertEquals(
					txt.trim(),
					cleanText.trim(),
					String.format("%s != %s", txt, cleanText)
			);			
		}
	}
	
	@Test
	public void extractText_Test() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		ICorpusBatchStep currentBatchStep = new io.outofprintmagazine.corpus.batch.impl.ExtractText();
		
		CorpusBatchStepModel corpusBatchStepModel = new CorpusBatchStepModel();
		corpusBatchStepModel.setCorpusId("Submissions");
		corpusBatchStepModel.setCorpusBatchId("OOPReading");
		corpusBatchStepModel.setCorpusBatchStepId("ExtractText");

		ObjectNode inputNode = mapper.createObjectNode();
		inputNode.put(
				"stagingLinkStorage",
				getStorage().storeScratchFileStream(
						corpusBatchStepModel.getCorpusId(), 
						getStorage().getScratchFilePath(
								corpusBatchStepModel.getCorpusBatchId(),
								corpusBatchStepModel.getCorpusBatchStepId(),
								"Story.docx"),
						getClass().getClassLoader().getResourceAsStream("io/outofprintmagazine/corpus/batch/impl/Story.docx")
				)
		);
		
		initCorpusBatchStep(currentBatchStep, corpusBatchStepModel);
		
		ArrayNode outputNodes = currentBatchStep.runOne(inputNode);
		assertTrue(outputNodes.size() > 0, "runOne returned nothing");
		for (JsonNode outputNode : outputNodes) {
			String txt = getStorage().getScratchFileString(
					corpusBatchStepModel.getCorpusId(),
					outputNode.get("stagingLinkStorage").asText()
			);
			assertTrue(
					txt.startsWith("Lorem ipsum"),
					String.format("%s != %s", txt, "Lorem ipsum")
			);			
		}

	}
	
	@Test
	public void generateDocID_Test() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		ICorpusBatchStep currentBatchStep = new io.outofprintmagazine.corpus.batch.impl.GenerateDocID();
		
		CorpusBatchStepModel corpusBatchStepModel = new CorpusBatchStepModel();
		corpusBatchStepModel.setCorpusId("Test");
		corpusBatchStepModel.setCorpusBatchId("Batch");
		corpusBatchStepModel.setCorpusBatchStepId("GenerateDocID");
		corpusBatchStepModel.setCorpusBatchStepSequenceId(new Integer(0));


		ObjectNode inputNode = mapper.createObjectNode();

		inputNode.put(
				"stagingLinkStorage",
				getStorage().storeScratchFileString(
						corpusBatchStepModel.getCorpusId(), 
						getStorage().getScratchFilePath(
								corpusBatchStepModel.getCorpusBatchId(),
								corpusBatchStepModel.getCorpusBatchStepId(),
								"DocID_Test.txt"),
						"Now is the time for all good men to come to the aid of their party."
				)
		);
		initCorpusBatchStep(currentBatchStep, corpusBatchStepModel);
		
		ArrayNode outputNodes1 = currentBatchStep.runOne(inputNode);
		ArrayNode outputNodes2 = currentBatchStep.runOne(inputNode);
		assertTrue(outputNodes1.size() == outputNodes2.size(), "runOne returned nothing");
		for (int i=0;i<outputNodes1.size()&&i<outputNodes2.size();i++) {
			assertEquals(
					outputNodes1.get(i).get("esnlc_DocIDAnnotation").asText(),
					outputNodes2.get(i).get("esnlc_DocIDAnnotation").asText(), 
					"DocID does not match"
			);
		}
	}

	
	@Test
	public void ebook_parseTOC_Test() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		ICorpusBatchStep currentBatchStep = new io.outofprintmagazine.corpus.batch.impl.ebook.ParseTOC();
		
		CorpusBatchStepModel corpusBatchStepModel = new CorpusBatchStepModel();
		corpusBatchStepModel.setCorpusId("Ebook");
		corpusBatchStepModel.setCorpusBatchId("Wodehouse");
		corpusBatchStepModel.setCorpusBatchStepId("ParseTOC");
		corpusBatchStepModel.setCorpusBatchStepSequenceId(new Integer(0));


		ObjectNode inputNode = mapper.createObjectNode();

		inputNode.put(
				"stagingLinkStorage",
				getStorage().storeScratchFileStream(
						corpusBatchStepModel.getCorpusId(), 
						getStorage().getScratchFilePath(
								corpusBatchStepModel.getCorpusBatchId(),
								corpusBatchStepModel.getCorpusBatchStepId(),
								"TOC.txt"),
						getClass().getClassLoader().getResourceAsStream("io/outofprintmagazine/corpus/batch/impl/ebook/Wodehouse.txt")
				)
		);

		
		initCorpusBatchStep(currentBatchStep, corpusBatchStepModel);
		
		ArrayNode outputNodes = currentBatchStep.runOne(inputNode);
		assertTrue(outputNodes.size() > 0, "runOne returned nothing");
		for (int i=0;i<outputNodes.size()-1;i++) {
			JsonNode outputNode = outputNodes.get(i);
			assertTrue(outputNode.has("nextTitle"), "missing end");
		}
	}
	
	@Test
	public void ebook_parseStory_Test() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		ICorpusBatchStep currentBatchStep = new io.outofprintmagazine.corpus.batch.impl.ebook.ParseStory();
		
		CorpusBatchStepModel corpusBatchStepModel = new CorpusBatchStepModel();
		corpusBatchStepModel.setCorpusId("Ebook");
		corpusBatchStepModel.setCorpusBatchId("Wodehouse");
		corpusBatchStepModel.setCorpusBatchStepId("ParseStory");
		corpusBatchStepModel.setCorpusBatchStepSequenceId(new Integer(0));

		ObjectNode inputNode = mapper.createObjectNode();
		inputNode.put(
				"stagingLinkStorage",
				getStorage().storeScratchFileStream(
						corpusBatchStepModel.getCorpusId(), 
						getStorage().getScratchFilePath(
								corpusBatchStepModel.getCorpusBatchId(),
								corpusBatchStepModel.getCorpusBatchStepId(),
								"Ebook.txt"),
						getClass().getClassLoader().getResourceAsStream("io/outofprintmagazine/corpus/batch/impl/ebook/Wodehouse.txt")
				)
		);
		inputNode.put("nextTitle", "Leave It to Jeeves");
		inputNode.put("esnlc_AuthorAnnotation", "P.G. Wodehouse");
		inputNode.put("esnlc_DocDateAnnotation", "1922");
		inputNode.put("esnlc_DocTitleAnnotation", "Extricating Young Gussie");
		
		initCorpusBatchStep(currentBatchStep, corpusBatchStepModel);
		
		ArrayNode outputNodes = currentBatchStep.runOne(inputNode);
		assertTrue(outputNodes.size() > 0, "runOne returned nothing");
		for (JsonNode outputNode : outputNodes) {
			checkStoryMetadata(outputNode);
			assertTrue(
					outputNode.has("stagingLinkStorage") 
					&& outputNode.get("stagingLinkStorage").asText().length() > 0
					&& !outputNode.get("stagingLinkStorage").asText().endsWith("Wodehouse.txt"), 
					"missing stagingLinkStorage"
			);
		}
	}
	
	@Test
	public void gutenberg_parseTableTOC_Test() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		ICorpusBatchStep currentBatchStep = new io.outofprintmagazine.corpus.batch.impl.gutenberg.ParseTableTOC();
		
		CorpusBatchStepModel corpusBatchStepModel = new CorpusBatchStepModel();
		corpusBatchStepModel.setCorpusId("Gutenberg");
		corpusBatchStepModel.setCorpusBatchId("Chekhov");
		corpusBatchStepModel.setCorpusBatchStepId("ParseTableTOC");
		corpusBatchStepModel.setCorpusBatchStepSequenceId(new Integer(0));


		ObjectNode inputNode = mapper.createObjectNode();
		inputNode.put("link", "https://www.gutenberg.org/files/13415/13415-h/13415-h.htm");
		inputNode.put(
				"stagingLinkStorage",
				getStorage().storeScratchFileStream(
						corpusBatchStepModel.getCorpusId(), 
						getStorage().getScratchFilePath(
								corpusBatchStepModel.getCorpusBatchId(),
								corpusBatchStepModel.getCorpusBatchStepId(),
								"Chekhov.html"),
						getClass().getClassLoader().getResourceAsStream("io/outofprintmagazine/corpus/batch/impl/gutenberg/Chekhov.html")
				)
		);

		
		initCorpusBatchStep(currentBatchStep, corpusBatchStepModel);
		
		ArrayNode outputNodes = currentBatchStep.runOne(inputNode);
		assertTrue(outputNodes.size() > 0, "runOne returned nothing");
		for (JsonNode outputNode : outputNodes) {
			assertTrue(outputNode.has("oop_Text"), "missing start");
			assertTrue(outputNode.has("oop_TextNext"), "missing end");
		}
	}
	
	@Test
	public void gutenberg_parseBodyStory_Test() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		ICorpusBatchStep currentBatchStep = new io.outofprintmagazine.corpus.batch.impl.gutenberg.ParseBodyStory();
		
		CorpusBatchStepModel corpusBatchStepModel = new CorpusBatchStepModel();
		corpusBatchStepModel.setCorpusId("Gutenberg");
		corpusBatchStepModel.setCorpusBatchId("Chekhov");
		corpusBatchStepModel.setCorpusBatchStepId("ParseBodyStory");
		corpusBatchStepModel.setCorpusBatchStepSequenceId(new Integer(0));

		ObjectNode inputNode = mapper.createObjectNode();
		inputNode.put("link", "https://www.gutenberg.org/files/13415/13415-h/13415-h.htm");
		inputNode.put(
				"stagingLinkStorage",
				getStorage().storeScratchFileStream(
						corpusBatchStepModel.getCorpusId(), 
						getStorage().getScratchFilePath(
								corpusBatchStepModel.getCorpusBatchId(),
								corpusBatchStepModel.getCorpusBatchStepId(),
								"Chekhov.html"),
						getClass().getClassLoader().getResourceAsStream("io/outofprintmagazine/corpus/batch/impl/gutenberg/Chekhov.html")
				)
		);
		inputNode.put("oop_Text", "a[name=THE_LADY_WITH_THE_DOG]");
		inputNode.put("oop_TextNext", "a[name=A_DOCTORS_VISIT]");
		inputNode.put("esnlc_AuthorAnnotation", "Anton Chekhov");
		inputNode.put("esnlc_DocDateAnnotation", "1899");
		inputNode.put("esnlc_DocTitleAnnotation", "THE LADY WITH THE DOG");
		
		initCorpusBatchStep(currentBatchStep, corpusBatchStepModel);
		try {
		ArrayNode outputNodes = currentBatchStep.runOne(inputNode);
		assertTrue(outputNodes.size() > 0, "runOne returned nothing");
		for (JsonNode outputNode : outputNodes) {
			checkStoryMetadata(outputNode);
			assertTrue(
					outputNode.has("stagingLinkStorage") 
					&& outputNode.get("stagingLinkStorage").asText().length() > 0
					&& !outputNode.get("stagingLinkStorage").asText().endsWith("Chekhov.html"), 
					"missing stagingLinkStorage"
			);
		}
		}
		catch (Throwable t) {
			t.printStackTrace();
			throw t;
		}
	}
	
	@Test
	public void gutenberg_parseTOC_Test() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		ICorpusBatchStep currentBatchStep = new io.outofprintmagazine.corpus.batch.impl.gutenberg.ParseTOC();
		
		CorpusBatchStepModel corpusBatchStepModel = new CorpusBatchStepModel();
		corpusBatchStepModel.setCorpusId("Gutenberg");
		corpusBatchStepModel.setCorpusBatchId("Maupassant");
		corpusBatchStepModel.setCorpusBatchStepId("ParseTOC");
		corpusBatchStepModel.setCorpusBatchStepSequenceId(new Integer(0));


		ObjectNode inputNode = mapper.createObjectNode();
		inputNode.put("link", "https://www.gutenberg.org/files/3080/3080-h/3080-h.htm");
		inputNode.put(
				"stagingLinkStorage",
				getStorage().storeScratchFileStream(
						corpusBatchStepModel.getCorpusId(), 
						getStorage().getScratchFilePath(
								corpusBatchStepModel.getCorpusBatchId(),
								corpusBatchStepModel.getCorpusBatchStepId(),
								"Maupassant.html"),
						getClass().getClassLoader().getResourceAsStream("io/outofprintmagazine/corpus/batch/impl/gutenberg/Maupassant.html")
				)
		);

		
		initCorpusBatchStep(currentBatchStep, corpusBatchStepModel);

		ArrayNode outputNodes = currentBatchStep.runOne(inputNode);

		assertTrue(outputNodes.size() > 0, "runOne returned nothing");
		for (JsonNode outputNode : outputNodes) {
			assertTrue(outputNode.has("oop_Text"), "missing start");
			assertTrue(outputNode.has("oop_TextNext"), "missing end");
		}

	}
	
	@Test
	public void gutenberg_parseStory_Test() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		ICorpusBatchStep currentBatchStep = new io.outofprintmagazine.corpus.batch.impl.gutenberg.ParseStory();
		
		CorpusBatchStepModel corpusBatchStepModel = new CorpusBatchStepModel();
		corpusBatchStepModel.setCorpusId("Gutenberg");
		corpusBatchStepModel.setCorpusBatchId("Maupassant");
		corpusBatchStepModel.setCorpusBatchStepId("ParseStory");
		corpusBatchStepModel.setCorpusBatchStepSequenceId(new Integer(0));

		ObjectNode inputNode = mapper.createObjectNode();
		inputNode.put("link", "https://www.gutenberg.org/files/3080/3080-h/3080-h.htm");
		inputNode.put(
				"stagingLinkStorage",
				getStorage().storeScratchFileStream(
						corpusBatchStepModel.getCorpusId(), 
						getStorage().getScratchFilePath(
								corpusBatchStepModel.getCorpusBatchId(),
								corpusBatchStepModel.getCorpusBatchStepId(),
								"Maupassant.html"),
						getClass().getClassLoader().getResourceAsStream("io/outofprintmagazine/corpus/batch/impl/gutenberg/Maupassant.html")
				)
		);
		inputNode.put("oop_Text", "a[name=link2H_4_0012]");
		inputNode.put("oop_TextNext", "a[name=link2H_4_0013]");
		inputNode.put("esnlc_AuthorAnnotation", "Guy de Maupassant");
		inputNode.put("esnlc_DocDateAnnotation", "1884");
		inputNode.put("esnlc_DocTitleAnnotation", "THE DIAMOND NECKLACE");
		
		initCorpusBatchStep(currentBatchStep, corpusBatchStepModel);

		ArrayNode outputNodes = currentBatchStep.runOne(inputNode);
		assertTrue(outputNodes.size() > 0, "runOne returned nothing");
		for (JsonNode outputNode : outputNodes) {
			checkStoryMetadata(outputNode);
			assertTrue(
					outputNode.has("stagingLinkStorage") 
					&& outputNode.get("stagingLinkStorage").asText().length() > 0
					&& !outputNode.get("stagingLinkStorage").asText().endsWith("Maupassant.html"), 
					"missing stagingLinkStorage"
			);
		}

	}
	
}
