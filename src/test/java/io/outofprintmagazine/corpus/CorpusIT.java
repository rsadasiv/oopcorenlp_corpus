package io.outofprintmagazine.corpus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatch;
import io.outofprintmagazine.corpus.batch.model.CorpusBatchModel;
import io.outofprintmagazine.corpus.batch.model.CorpusBatchStepModel;
import io.outofprintmagazine.util.IParameterStore;
import io.outofprintmagazine.util.ParameterStoreLocal;

public class CorpusIT {
	
	private static CorpusBatchModel chekhovBatchCandidate = null;
	private static CorpusBatchModel maupassantBatchCandidate = null;
	private static CorpusBatchModel wodehouseBatchCandidate = null;
	private static CorpusBatchModel ohenryBatchCandidate = null;
	private static CorpusBatchModel chekhovBatchGoldSource = null;
	private static CorpusBatchModel maupassantBatchGoldSource = null;
	private static CorpusBatchModel wodehouseBatchGoldSource = null;
	private static CorpusBatchModel ohenryBatchGoldSource = null;

	private static final String fileStaging_Path = "../Staging_IT";
	
	public CorpusIT() throws Exception {
		super();
	}
	
	private IParameterStore parameterStore = null;

	public IParameterStore getParameterStore() throws Exception {
		if (parameterStore == null) {
			ObjectMapper mapper = new ObjectMapper();
			ObjectNode p = mapper.createObjectNode();
			p.put("wordNet_location", "../data/dict");
			p.put("verbNet_location", "../data/verbnet3.3");
			p.put("wikipedia_apikey", "OOPCoreNlp/1.0 httpclient/4.5.6");
			p.put("fileCorpus_Path", "../Corpora_IT");
			parameterStore = new ParameterStoreLocal(p);
		}
		return parameterStore;
	}
	
	@BeforeAll
	private static void init() throws Exception {
		try {
			FileUtils.deleteDirectory(new File("../Staging_IT"));
			File dir = new File("../Staging_IT");
			if (!dir.exists()) dir.mkdirs();
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
		try {
			FileUtils.deleteDirectory(new File("../Corpora_IT"));
			File dir = new File("../Corpora_IT");
			if (!dir.exists()) dir.mkdirs();
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
		Properties p = new Properties();
		p.put("wordNet_location", "../data/dict");
		p.put("verbNet_location", "../data/verbnet3.3");
		p.put("wikipedia_apikey", "OOPCoreNlp/1.0 httpclient/4.5.6");
		p.put("fileCorpus_Path", "../Corpora_IT");
		FileOutputStream fout = null;
		try {
			fout = new FileOutputStream(
					new File(
							fileStaging_Path
							+ System.getProperty("file.separator", "/")
							+ "oopcorenlp.properties"
					)
			);
			p.store(fout, "Integration Test");
		}
		finally {
			if (fout != null) {
				fout.close();
			}
		}
		getChekhovBatchCandidate();
		getMaupassantBatchCandidate();
		getWodehouseBatchCandidate();
		getOHenryBatchCandidate();
		getChekhovBatchGoldSource();
		getMaupassantBatchGoldSource();
		getWodehouseBatchGoldSource();
		getOHenryBatchGoldSource();
	}
	
	private static void writeFile(String path, String fileName, String content) throws IOException {
		FileOutputStream fout = null;
		try {
			File f = new File(path + System.getProperty("file.separator", "/") + fileName);
			if (!f.getParentFile().exists()) {
				f.getParentFile().mkdirs();
			}
			fout = new FileOutputStream(f);
			fout.write(content.getBytes());
		}
		finally {
			if (fout != null) {
				fout.close();
				fout.flush();
				fout = null;
			}
		}
	}
	
	private static void writeFile(String path, String fileName, JsonNode content) throws IOException {
		File f = new File(path + System.getProperty("file.separator", "/") + fileName);
		ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		mapper.configure(com.fasterxml.jackson.core.JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
		mapper.writeValue(f, content);
	}
	
	private static void writeFile(String path, String fileName, InputStream in) throws IOException {
		File f = new File(path + System.getProperty("file.separator", "/") + fileName);
		FileOutputStream fout = null;
		try {
			fout = new FileOutputStream(f);
			IOUtils.copy(in,fout);
		}
		finally {
			in.close();
			if (fout != null) {
				fout.flush();
				fout.close();
			}
		}
	}
		
	private static void getChekhovBatchCandidate() throws Exception {
		if (chekhovBatchCandidate == null) {
			writeFile(
					fileStaging_Path, 
					"Chekhov.html", 
					CorpusIT.class.getClassLoader().getResourceAsStream("io/outofprintmagazine/corpus/batch/impl/gutenberg/Chekhov.html")
			);
			writeFile(
					fileStaging_Path, 
					"Chekhov.html.properties", 
					CorpusIT.class.getClassLoader().getResourceAsStream("io/outofprintmagazine/corpus/batch/impl/gutenberg/Chekhov.html.properties")
			);
			CorpusBatch chekhovBatch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/gutenberg/Chekhov.json");
			chekhovBatch.run();
			chekhovBatchCandidate = chekhovBatch.getData();
		}
	}
	
	private static void getMaupassantBatchCandidate() throws Exception {
		if (maupassantBatchCandidate == null) {
			writeFile(
					fileStaging_Path, 
					"Maupassant.html", 
					CorpusIT.class.getClassLoader().getResourceAsStream("io/outofprintmagazine/corpus/batch/impl/gutenberg/Maupassant.html")
			);
			writeFile(
					fileStaging_Path, 
					"Maupassant.html.properties", 
					CorpusIT.class.getClassLoader().getResourceAsStream("io/outofprintmagazine/corpus/batch/impl/gutenberg/Maupassant.html.properties")
			);
			CorpusBatch maupassantBatch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/gutenberg/Maupassant.json");
			maupassantBatch.run();
			maupassantBatchCandidate = maupassantBatch.getData();
		}
	}
	
	private static void getWodehouseBatchCandidate() throws Exception {
		if (wodehouseBatchCandidate == null) {
			writeFile(
					fileStaging_Path, 
					"Wodehouse.txt", 
					CorpusIT.class.getClassLoader().getResourceAsStream("io/outofprintmagazine/corpus/batch/impl/ebook/Wodehouse.txt")
			);
			writeFile(
					fileStaging_Path, 
					"Wodehouse.txt.properties", 
					CorpusIT.class.getClassLoader().getResourceAsStream("io/outofprintmagazine/corpus/batch/impl/ebook/Wodehouse.txt.properties")
			);
			CorpusBatch wodehouseBatch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/ebook/Wodehouse.json");
			wodehouseBatch.run();
			wodehouseBatchCandidate = wodehouseBatch.getData();
		}
	}
	
	private static void getOHenryBatchCandidate() throws Exception {
		if (ohenryBatchCandidate == null) {
			CorpusBatch ohenryBatch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/wikisource/OHenry.json");
			ohenryBatch.run();
			ohenryBatchCandidate = ohenryBatch.getData();
		}
	}
	
	private static void getChekhovBatchGoldSource() throws IOException {
		if (chekhovBatchGoldSource == null) {
			chekhovBatchGoldSource = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/gutenberg/ChekhovBatch.json").getData();
		}
	}
	
	private static void getMaupassantBatchGoldSource() throws IOException {
		if (maupassantBatchGoldSource == null) {
			maupassantBatchGoldSource = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/gutenberg/MaupassantBatch.json").getData();
		}
	}
	
	private static void getWodehouseBatchGoldSource() throws IOException {
		if (wodehouseBatchGoldSource == null) {
			wodehouseBatchGoldSource = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/ebook/WodehouseBatch.json").getData();
		}
	}
	
	private static void getOHenryBatchGoldSource() throws IOException {
		if (ohenryBatchGoldSource == null) {
			ohenryBatchGoldSource = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/wikisource/OHenryBatch.json").getData();
		}
	}
	    

	
	List<String> skipFieldValues = Arrays.asList(
			"esnlc_DocDateAnnotation",
			"stagingLinkStorage"
	);
	
	private void testInputContents(CorpusBatchStepModel gold, CorpusBatchStepModel candidate) {
		for (int i=0;i<gold.getInput().size() && i<candidate.getInput().size();i++) {
			JsonNode goldItem = gold.getInput().get(i);
			JsonNode candidateItem = candidate.getInput().get(i);
			Iterator<String> goldFieldNamesIter = goldItem.fieldNames();
			while (goldFieldNamesIter.hasNext()) {
				String goldFieldName = goldFieldNamesIter.next();
				assertTrue(candidateItem.has(goldFieldName), String.format("Input missing field: %s", goldFieldName));
				if (!skipFieldValues.contains(goldFieldName)) {
					assertEquals(goldItem.get(goldFieldName), candidateItem.get(goldFieldName), String.format("Input value mismatch: %s", goldFieldName));
				}
			}
		}		
	}
	
	private void testOutputContents(CorpusBatchStepModel gold, CorpusBatchStepModel candidate) {
		for (int i=0;i<gold.getInput().size() && i<candidate.getInput().size();i++) {
			JsonNode goldItem = gold.getOutput().get(i);
			JsonNode candidateItem = candidate.getOutput().get(i);
			Iterator<String> goldFieldNamesIter = goldItem.fieldNames();
			while (goldFieldNamesIter.hasNext()) {
				String goldFieldName = goldFieldNamesIter.next();
				assertTrue(candidateItem.has(goldFieldName), String.format("Input missing field: %s", goldFieldName));
				if (!skipFieldValues.contains(goldFieldName)) {
					assertEquals(goldItem.get(goldFieldName), candidateItem.get(goldFieldName), String.format("Input value mismatch: %s", goldFieldName));
				}
			}
		}		
	}
	
	@Test
	public void chekhovImportDirectoryInputSizeIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(0);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(0);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "ImportDirectory input mismatch");
	}
	
	
	@Test
	public void chekhovImportDirectoryInputContentsIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(0);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(0);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void chekhovImportDirectoryOutputSizeIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(0);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(0);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "ImportDirectory output mismatch");
	}
	
	@Test
	public void chekhovImportDirectoryOutputContentsIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(0);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(0);
		testOutputContents(gold, candidate);
	}
	
	@Test
	public void chekhovParseTableTOCInputSizeIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(1);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(1);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "ParseTableTOC input mismatch");
	}
	
	@Test
	public void chekhovParseTableTOCInputContentsIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(1);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(1);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void chekhovParseTableTOCOutputSizeIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(1);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(1);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "ParseTableTOC output mismatch");
	}
	
	@Test
	public void chekhovParseTableTOCOutputContentsIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(1);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(1);
		testOutputContents(gold, candidate);
	}
	
	@Test
	public void chekhovParseBodyStoryInputSizeIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(2);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(2);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "ParseBodyStory input mismatch");
	}
	
	@Test
	public void chekhovParseBodyStoryInputContentsIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(2);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(2);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void chekhovParseBodyStoryOutputSizeIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(2);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(2);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "ParseBodyStory output mismatch");
	}
	
	@Test
	public void chekhovParseBodyStoryOutputContentsIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(2);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(2);
		testOutputContents(gold, candidate);
	}
	
	@Test
	public void chekhovCleanTextInputSizeIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(3);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(3);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "CleanText input mismatch");
	}
	
	@Test
	public void chekhovCleanTextInputContentsIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(3);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(3);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void chekhovCleanTextOutputSizeIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(3);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(3);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "CleanText output mismatch");
	}
	
	@Test
	public void chekhovCleanTextOutputContentsIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(3);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(3);
		testOutputContents(gold, candidate);
	}
	
	@Test
	public void chekhovGenerateDocIDInputSizeIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(4);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(4);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "GenerateDocID input mismatch");
	}
	
	@Test
	public void chekhovGenerateDocIDInputContentsIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(4);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(4);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void chekhovGenerateDocIDOutputSizeIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(4);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(4);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "GenerateDocID output mismatch");
	}
	
	@Test
	public void chekhovGenerateDocIDOutputContentsIT_Test() {
		CorpusBatchStepModel gold = chekhovBatchGoldSource.getCorpusBatchSteps().get(4);
		CorpusBatchStepModel candidate = chekhovBatchCandidate.getCorpusBatchSteps().get(4);
		testOutputContents(gold, candidate);
	}
	
	@Test
	public void maupassantImportDirectoryInputSizeIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(0);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(0);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "ImportDirectory input mismatch");
	}
	
	@Test
	public void maupassantImportDirectoryInputContentsIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(0);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(0);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void maupassantImportDirectoryOutputSizeIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(0);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(0);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "ImportDirectory output mismatch");
	}
	
	@Test
	public void maupassantImportDirectoryOutputContentsIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(0);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(0);
		testOutputContents(gold, candidate);
	}
	
	@Test
	public void maupassantParseTOCInputSizeIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(1);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(1);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "ParseTOC input mismatch");
	}
	
	@Test
	public void maupassantParseTOCInputContentsIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(1);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(1);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void maupassantParseTOCOutputSizeIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(1);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(1);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "ParseTOC output mismatch");
	}
	
	@Test
	public void maupassantParseTOCOutputContentsIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(1);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(1);
		testOutputContents(gold, candidate);
	}
	
	@Test
	public void maupassantParseStoryInputSizeIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(2);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(2);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "ParseStory input mismatch");
	}
	
	@Test
	public void maupassantParseStoryInputContentsIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(2);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(2);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void maupassantParseStoryOutputSizeIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(2);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(2);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "ParseStory output mismatch");
	}
	
	@Test
	public void maupassantParseStoryOutputContentsIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(2);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(2);
		testOutputContents(gold, candidate);
	}
	
	@Test
	public void maupassantCleanTextInputSizeIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(3);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(3);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "CleanText input mismatch");
	}
	
	@Test
	public void maupassantCleanTextInputContentsIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(3);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(3);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void maupassantCleanTextOutputSizeIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(3);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(3);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "CleanText output mismatch");
	}
	
	@Test
	public void maupassantCleanTextOutputContentsIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(3);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(3);
		testOutputContents(gold, candidate);
	}
	
	@Test
	public void maupassantGenerateDocIDInputSizeIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(4);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(4);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "GenerateDocID input mismatch");
	}
	
	@Test
	public void maupassantGenerateDocIDInputContentsIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(4);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(4);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void maupassantGenerateDocIDOutputSizeIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(4);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(4);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "GenerateDocID output mismatch");
	}
	
	@Test
	public void maupassantGenerateDocIDOutputContentsIT_Test() {
		CorpusBatchStepModel gold = maupassantBatchGoldSource.getCorpusBatchSteps().get(4);
		CorpusBatchStepModel candidate = maupassantBatchCandidate.getCorpusBatchSteps().get(4);
		testOutputContents(gold, candidate);
	}
	
	@Test
	public void wodehouseImportDirectoryInputSizeIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(0);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(0);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "ImportDirectory input mismatch");
	}
	
	@Test
	public void wodehouseImportDirectoryInputContentsIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(0);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(0);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void wodehouseImportDirectoryOutputSizeIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(0);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(0);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "ImportDirectory output mismatch");
	}
	
	@Test
	public void wodehouseImportDirectoryOutputContentsIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(0);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(0);
		testOutputContents(gold, candidate);
	}
	
	@Test
	public void wodehouseGenerateTOCInputSizeIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(1);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(1);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "GenerateTOC input mismatch");
	}
	
	@Test
	public void wodehouseGenerateTOCInputContentsIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(1);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(1);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void wodehouseGenerateTOCOutputSizeIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(1);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(1);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "GenerateTOC output mismatch");
	}
	
	@Test
	public void wodehouseGenerateTOCOutputContentsIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(1);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(1);
		testOutputContents(gold, candidate);
	}
	
	@Test
	public void wodehouseParseStoryInputSizeIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(2);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(2);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "ParseStory input mismatch");
	}
	
	@Test
	public void wodehouseParseStoryInputContentsIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(2);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(2);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void wodehouseParseStoryOutputSizeIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(2);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(2);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "ParseStory output mismatch");
	}
	
	
	@Test
	public void wodehouseParseStoryOutputContentsIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(2);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(2);
		testOutputContents(gold, candidate);
	}
	
	@Test
	public void wodehouseCleanTextInputSizeIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(3);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(3);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "CleanText input mismatch");
	}
	
	@Test
	public void wodehouseCleanTextInputContentsIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(3);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(3);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void wodehouseCleanTextOutputSizeIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(3);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(3);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "CleanText output mismatch");
	}
	
	@Test
	public void wodehouseCleanTextOutputContentsIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(3);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(3);
		testOutputContents(gold, candidate);
	}
	
	@Test
	public void wodehouseGenerateDocIDInputSizeIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(4);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(4);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "GenerateDocID input mismatch");
	}
	
	@Test
	public void wodehouseGenerateDocIDInputContentsIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(4);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(4);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void wodehouseGenerateDocIDOutputSizeIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(4);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(4);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "GenerateDocID output mismatch");
	}
	
	@Test
	public void wodehouseGenerateDocIDOutputContentsIT_Test() {
		CorpusBatchStepModel gold = wodehouseBatchGoldSource.getCorpusBatchSteps().get(4);
		CorpusBatchStepModel candidate = wodehouseBatchCandidate.getCorpusBatchSteps().get(4);
		testOutputContents(gold, candidate);
	}
	
	@Test
	public void ohenryDownloadTOCInputSizeIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(0);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(0);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "DownloadTOC input mismatch");
	}
	
	@Test
	public void ohenryDownloadTOCInputContentsIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(0);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(0);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void ohenryDownloadTOCOutputSizeIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(0);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(0);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "DownloadTOC output mismatch");
	}
	
	@Test
	public void ohenryDownloadTOCOutputContentsIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(0);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(0);
		testOutputContents(gold, candidate);
	}
	
	@Test
	public void ohenryParseTOCInputSizeIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(1);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(1);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "ParseTOC input mismatch");
	}
	
	@Test
	public void ohenryParseTOCInputContentsIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(1);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(1);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void ohenryParseTOCOutputSizeIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(1);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(1);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "ParseTOC output mismatch");
	}
	
	@Test
	public void ohenryParseTOCOutputContentsIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(1);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(1);
		testOutputContents(gold, candidate);
	}
	
	@Test
	public void ohenryDownloadStoryInputSizeIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(2);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(2);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "DownloadStory input mismatch");
	}
	
	@Test
	public void ohenryDownloadStoryInputContentsIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(2);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(2);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void ohenryDownloadStoryOutputSizeIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(2);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(2);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "DownloadStory output mismatch");
	}
	
	@Test
	public void ohenryDownloadStoryOutputContentsIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(2);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(2);
		testOutputContents(gold, candidate);
	}

	@Test
	public void ohenryParseStoryInputSizeIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(3);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(3);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "ParseStory input mismatch");
	}
	
	@Test
	public void ohenryParseStoryInputContentsIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(3);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(3);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void ohenryParseStoryOutputSizeIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(3);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(3);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "ParseStory output mismatch");
	}	
	
	@Test
	public void ohenryParseStoryOutputContentsIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(3);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(3);
		testOutputContents(gold, candidate);
	}	
	
	@Test
	public void ohenryCleanTextInputSizeIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(4);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(4);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "CleanText input mismatch");
	}
	
	@Test
	public void ohenryCleanTextInputContentsIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(4);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(4);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void ohenryCleanTextOutputSizeIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(4);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(4);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "CleanText output mismatch");
	}
	
	@Test
	public void ohenryCleanTextOutputContentsIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(4);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(4);
		testOutputContents(gold, candidate);
	}
	
	@Test
	public void ohenryGenerateDocIDInputSizeIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(5);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(5);
		assertEquals(gold.getInput().size(), candidate.getInput().size(), "GenerateDocID input mismatch");
	}
	
	@Test
	public void ohenryGenerateDocIDInputContentsIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(5);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(5);
		testInputContents(gold, candidate);
	}
	
	@Test
	public void ohenryGenerateDocIDOutputSizeIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(4);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(4);
		assertEquals(gold.getOutput().size(), candidate.getOutput().size(), "GenerateDocID output mismatch");
	}
	
	@Test
	public void ohenryGenerateDocIDOutputContentsIT_Test() {
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(4);
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(4);
		testOutputContents(gold, candidate);
	}

}
