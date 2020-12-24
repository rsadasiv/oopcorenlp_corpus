package io.outofprintmagazine.corpus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatch;
import io.outofprintmagazine.corpus.batch.model.CorpusBatchModel;
import io.outofprintmagazine.corpus.batch.model.CorpusBatchStepModel;
import io.outofprintmagazine.util.IParameterStore;
import io.outofprintmagazine.util.ParameterStoreLocal;

@TestMethodOrder(MethodOrderer.Alphanumeric.class)
public class CorpusIT {
	
	
	
	private static JsonNode sampleAggregate = null;
	private static JsonNode sampleCorpusAggregate = null;
	private static JsonNode sampleAllCorporaAggregate = null;
	private static CorpusBatchModel chekhovBatchCandidate = null;
	private static CorpusBatchModel maupassantBatchCandidate = null;
	private static CorpusBatchModel wodehouseBatchCandidate = null;
	private static CorpusBatchModel ohenryBatchCandidate = null;
	private static CorpusBatchModel chekhovBatchGoldSource = null;
	private static CorpusBatchModel maupassantBatchGoldSource = null;
	private static CorpusBatchModel wodehouseBatchGoldSource = null;
	private static CorpusBatchModel ohenryBatchGoldSource = null;

	private static final String fileStaging_Path = "../Staging_IT";
	private static final String fileCorpus_Path = "../Corpora_IT";
	
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
			p.put("fileCorpus_Path", fileCorpus_Path);
			parameterStore = new ParameterStoreLocal(p);
		}
		return parameterStore;
	}
	
	List<String> skipFieldValues = Arrays.asList(
			"esnlc_DocDateAnnotation",
			"stagingLinkStorage",
			"stagingLocation"
	);
	
	@BeforeAll
	private static void init() throws Exception {
		try {
			FileUtils.deleteDirectory(new File(fileStaging_Path));
			Thread.sleep(5);
			File dir = new File(fileStaging_Path);
			if (!dir.exists()) dir.mkdirs();
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
		try {
			FileUtils.deleteDirectory(new File(fileCorpus_Path));
			Thread.sleep(5);
			File dir = new File(fileCorpus_Path);
			if (!dir.exists()) dir.mkdirs();
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
		Properties p = new Properties();
		p.put("wordNet_location", "../data/dict");
		p.put("verbNet_location", "../data/verbnet3.3");
		p.put("wikipedia_apikey", "OOPCoreNlp/1.0 httpclient/4.5.6");
		p.put("fileCorpus_Path", fileCorpus_Path);
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
	
	//MethodOrderer.Alphanumeric ensures that all _getXXX test methods are executed first
	
	@Test
	public void _getSampleCorpusCandidate() throws Exception {
		ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		if (sampleAggregate == null || sampleCorpusAggregate == null || sampleAllCorporaAggregate == null) {
			writeFile(
					fileStaging_Path, 
					"Sample.txt", 
					CorpusIT.class.getClassLoader().getResourceAsStream("io/outofprintmagazine/util/story.txt")
			);
			writeFile(
					fileStaging_Path, 
					"Sample.txt.properties", 
					CorpusIT.class.getClassLoader().getResourceAsStream("io/outofprintmagazine/util/metadata.properties")
			);
			CorpusBatch sampleBatch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/sample/SampleBatch.json");
			sampleBatch.appendAnalyzeStep();
			sampleBatch.appendAggregateStep();
			sampleBatch.run();

			CorpusBatch corporaAggregatesBatch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/corporaAggregates/CorporaAggregates.json");
			try {
				List<CorpusBatch> corpora = new ArrayList<CorpusBatch>();
				corpora.add(sampleBatch);
				corporaAggregatesBatch.aggregateBatches(corpora);
				corporaAggregatesBatch.run();
			}
			catch (Exception e) {
				e.printStackTrace();
				throw e;
			}

			//get documentAggregate and corpusAggregate as JsonNode

			CorpusBatchStepModel finalStep = sampleBatch.getData().getCorpusBatchSteps().get(sampleBatch.getData().getCorpusBatchSteps().size()-1);
			
			sampleAggregate = mapper.readTree(
					new File(
							fileCorpus_Path
							+ System.getProperty("file.separator", "/")
							+ sampleBatch.getData().getCorpusId()
							+ System.getProperty("file.separator", "/")
							+ finalStep.getOutput().get(0).get("oopNLPAggregatesStorage").asText()
					)
			);
			sampleCorpusAggregate = mapper.readTree(
					new File(
							fileCorpus_Path
							+ System.getProperty("file.separator", "/")
							+ sampleBatch.getData().getCorpusId()
							+ System.getProperty("file.separator", "/")
							+ finalStep.getOutput().get(0).get("oopNLPCorpusAggregatesStorage").asText()
					)
			);

			sampleAllCorporaAggregate = mapper.readTree(
					new File(
							fileCorpus_Path
							+ System.getProperty("file.separator", "/")
							+ corporaAggregatesBatch.getData().getCorpusId()
							+ System.getProperty("file.separator", "/")
							+ corporaAggregatesBatch.getData().getCorpusBatchSteps().get(corporaAggregatesBatch.getData().getCorpusBatchSteps().size()-1).getOutput().get(0).get("oopNLPCorpusAggregatesStorage").asText()
					)
			);
		}		
	}
	
	@Test
	public void _getChekhovBatchCandidate() throws Exception {
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
	
	@Test
	public void _getMaupassantBatchCandidate() throws Exception {
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
	
	@Test
	public void _getWodehouseBatchCandidate() throws Exception {
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
	
	@Test
	public void _getOHenryBatchCandidate() throws Exception {
		if (ohenryBatchCandidate == null) {
			CorpusBatch ohenryBatch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/wikisource/OHenry.json");
			ohenryBatch.run();
			ohenryBatchCandidate = ohenryBatch.getData();
		}
	}
	
	public static void getChekhovBatchGoldSource() throws IOException {
		if (chekhovBatchGoldSource == null) {
			chekhovBatchGoldSource = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/gutenberg/ChekhovBatch.json").getData();
		}
	}
	
	public static void getMaupassantBatchGoldSource() throws IOException {
		if (maupassantBatchGoldSource == null) {
			maupassantBatchGoldSource = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/gutenberg/MaupassantBatch.json").getData();
		}
	}
	
	public static void getWodehouseBatchGoldSource() throws IOException {
		if (wodehouseBatchGoldSource == null) {
			wodehouseBatchGoldSource = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/ebook/WodehouseBatch.json").getData();
		}
	}
	
	public static void getOHenryBatchGoldSource() throws IOException {
		if (ohenryBatchGoldSource == null) {
			ohenryBatchGoldSource = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/wikisource/OHenryBatch.json").getData();
		}
	}
	
	//there is only one document in sample corpus, so document aggregate should equal corpus aggregate
	private void checkAggregateScore(JsonNode documentAggregate, JsonNode corpusAggregate, String annotationName) {
		Iterator<String> scoreNameIter = documentAggregate.get(annotationName).get("scoreStats").get("score").fieldNames();
		while (scoreNameIter.hasNext()) {
			String scoreName = scoreNameIter.next();
			Iterator<String> statNameIter = corpusAggregate.get(annotationName).get("score").get(scoreName).fieldNames();
			while (statNameIter.hasNext()) {
				String statName = statNameIter.next();
				//stddev is hosed
				if (!statName.equals("stddev")) {
					assertEquals(
							documentAggregate.get(annotationName).get("scoreStats").get("score").get(scoreName).asText(),
							corpusAggregate.get(annotationName).get("score").get(scoreName).get(statName).asText(),
							String.format(
									"Aggregate value mismatch: %s %s %s"
									, annotationName
									, scoreName
									, statName
							)
					);
				}
			}
		}
	}
	
	//there is only one corpus in sample corpora, so corpus aggregate should equal corpora aggregate
	private void checkCorpusAggregateScore(JsonNode corpusAggregate, JsonNode corporaAggregate, String annotationName) {
		List<String> statNames = Arrays.asList("raw", "normalized", "count");
		Iterator<String> statNameIter = statNames.iterator();
		while (statNameIter.hasNext()) {
			String statName = statNameIter.next();
			Iterator<String> scoreNameIter = corpusAggregate.get(annotationName).get("score").get(statName).fieldNames();
			while (scoreNameIter.hasNext()) {
				String scoreName = scoreNameIter.next();
				assertEquals(
						corpusAggregate.get(annotationName).get("score").get(statName).get(scoreName).asText(),
						corporaAggregate.get(annotationName).get("score").get(statName).get(scoreName).asText(),
						String.format(
								"CorpusAggregate value mismatch: %s %s %s"
								, annotationName
								, scoreName
								, statName
						)
				);
			}
		}
	}
	
	private void checkInputContents(CorpusBatchStepModel gold, CorpusBatchStepModel candidate) {
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
	
	private void checkOutputContents(CorpusBatchStepModel gold, CorpusBatchStepModel candidate) {
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
	public void sampleAggregatesCoreNlpSentimentAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "CoreNlpSentimentAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPActionlessVerbsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPActionlessVerbsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPAdjectiveCategoriesAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPAdjectiveCategoriesAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPAdjectivesAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPAdjectivesAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPAdverbCategoriesAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPAdverbCategoriesAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPAdverbsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPAdverbsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPAmericanizeAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPAmericanizeAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPAngliciseAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPAngliciseAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPBiberAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPBiberAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPBiberDimensionsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPBiberDimensionsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPCharCountAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPCharCountAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPColorsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPColorsAnnotation");
	}

	@Test
	public void sampleAggregatesOOPCommonWordsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPCommonWordsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPFlavorsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPFlavorsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPFleschKincaidAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPFleschKincaidAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPFunctionWordsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPFunctionWordsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPGenderAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPGenderAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPMyersBriggsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPMyersBriggsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPNounGroupsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPNounGroupsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPNounHypernymsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPNounHypernymsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPNounsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPNounsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPParagraphCountAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPParagraphCountAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPPointlessAdjectivesAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPPointlessAdjectivesAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPPointlessAdverbsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPPointlessAdverbsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPPossessivesAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPPossessivesAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPPrepositionCategoriesAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPPrepositionCategoriesAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPPrepositionsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPPrepositionsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPPronounAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPPronounAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPPunctuationMarkAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPPunctuationMarkAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPSVOAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPSVOAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPSentenceCountAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPSentenceCountAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPSyllableCountAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPSyllableCountAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPTokenCountAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPTokenCountAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPTopicsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPTopicsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPUncommonWordsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPUncommonWordsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPVerbGroupsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPVerbGroupsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPVerbHypernymsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPVerbHypernymsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPVerbTenseAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPVerbTenseAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPVerblessSentencesAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPVerblessSentencesAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPVerbnetGroupsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPVerbnetGroupsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPVerbsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPVerbsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPWikipediaPageviewTopicsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPWikipediaPageviewTopicsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPWordCountAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPWordCountAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPWordlessWordsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPWordlessWordsAnnotation");
	}
	
	@Test
	public void sampleAggregatesOOPWordsAnnotation_Test() {
		checkAggregateScore(sampleAggregate, sampleCorpusAggregate, "OOPWordsAnnotation");
	}
	
	
	@Test
	public void sampleAllCorporaAggregateCoreNlpSentimentAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "CoreNlpSentimentAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPActionlessVerbsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPActionlessVerbsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregatesOOPAdjectiveCategoriesAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPAdjectiveCategoriesAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPAdjectivesAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPAdjectivesAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPAdverbCategoriesAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPAdverbCategoriesAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPAdverbsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPAdverbsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPAmericanizeAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPAmericanizeAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPAngliciseAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPAngliciseAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPBiberAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPBiberAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPBiberDimensionsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPBiberDimensionsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPCharCountAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPCharCountAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPColorsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPColorsAnnotation");
	}

	@Test
	public void sampleAllCorporaAggregateOOPCommonWordsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPCommonWordsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPFlavorsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPFlavorsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPFleschKincaidAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPFleschKincaidAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPFunctionWordsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPFunctionWordsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPGenderAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPGenderAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPMyersBriggsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPMyersBriggsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPNounGroupsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPNounGroupsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPNounHypernymsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPNounHypernymsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPNounsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPNounsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPParagraphCountAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPParagraphCountAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPPointlessAdjectivesAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPPointlessAdjectivesAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPPointlessAdverbsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPPointlessAdverbsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPPossessivesAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPPossessivesAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPPrepositionCategoriesAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPPrepositionCategoriesAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPPrepositionsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPPrepositionsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPPronounAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPPronounAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPPunctuationMarkAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPPunctuationMarkAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPSVOAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPSVOAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPSentenceCountAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPSentenceCountAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPSyllableCountAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPSyllableCountAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPTokenCountAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPTokenCountAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPTopicsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPTopicsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPUncommonWordsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPUncommonWordsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPVerbGroupsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPVerbGroupsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPVerbHypernymsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPVerbHypernymsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPVerbTenseAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPVerbTenseAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPVerblessSentencesAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPVerblessSentencesAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPVerbnetGroupsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPVerbnetGroupsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPVerbsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPVerbsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPWikipediaPageviewTopicsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPWikipediaPageviewTopicsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPWordCountAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPWordCountAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPWordlessWordsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPWordlessWordsAnnotation");
	}
	
	@Test
	public void sampleAllCorporaAggregateOOPWordsAnnotation_Test() {
		checkCorpusAggregateScore(sampleCorpusAggregate, sampleAllCorporaAggregate, "OOPWordsAnnotation");
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
		checkInputContents(gold, candidate);
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
		checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		//checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		//checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		//checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		checkOutputContents(gold, candidate);
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
		checkInputContents(gold, candidate);
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
		//checkOutputContents(gold, candidate);
	}
	
	
	public static void main(String[] argv) throws Exception {
		init();
		CorpusIT me = new CorpusIT();
		ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		CorpusBatchStepModel gold = ohenryBatchGoldSource.getCorpusBatchSteps().get(0);
		System.out.println("gold");
		System.out.println(mapper.writeValueAsString(gold));
		CorpusBatchStepModel candidate = ohenryBatchCandidate.getCorpusBatchSteps().get(0);
		System.out.println("candidate");
		System.out.println(mapper.writeValueAsString(candidate));
		//me.ohenryDownloadTOCInputSizeIT_Test();
		//me.ohenryDownloadTOCOutputSizeIT_Test();
	}

}
