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
package io.outofprintmagazine.corpus;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.outofprintmagazine.corpus.batch.CorpusBatch;
import io.outofprintmagazine.corpus.storage.ScratchStorage;
import io.outofprintmagazine.corpus.storage.file.FileScratchStorage;
import io.outofprintmagazine.corpus.storage.postgresql.PostgreSQLBatchStorage;
import io.outofprintmagazine.corpus.storage.s3.S3BatchStorage;
import io.outofprintmagazine.corpus.util.ParameterStorePropertiesFile;

public class App
{
	private static final Logger logger = LogManager.getLogger(App.class);
	
    public static void main(String[] argv ) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
    	//CorpusStorage storage = new FileCorpora();
    	CorpusBatch corpusBatch = new CorpusBatch();
    	ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
//    	corpusBatch.setData(
//    			mapper.readValue(
//    					//corpusBatch.getClass().getClassLoader().getResourceAsStream("io/outofprintmagazine/corpus/batch/oopArchive/ArchiveBatch.json"),
//    					corpusBatch.getClass().getClassLoader().getResourceAsStream("io/outofprintmagazine/corpus/batch/oopArchive/DNASectionBatch.json"),
//    					CorpusBatchModel.class
//    			)
//    	);
    	

    	
    	
    	//ObjectNode stagingBatch = storage.getStagingBatch("DNA", "DNALifestyle");
//    	ObjectNode stagingBatch = storage.getStagingBatch("Published", "OOPArchives");
//    	
//    	System.out.println(mapper.writeValueAsString(stagingBatch));
//    	corpusBatch.setData(mapper.treeToValue(stagingBatch, CorpusBatchModel.class));
//    	
//    	System.out.println(corpusBatch.getData().getCorpusId());
//    	System.out.println(corpusBatch.getData().getCorpusBatchId());
//    	corpusBatch.runStep("CleanStoryText");
    	App app = new App();
//    	app.testAO3AtomTemplate();
//    	app.testAO3Template();
//    	app.testDNALifestyleTemplate();
//    	app.testDNALiteratureTemplate();
    	app.testDropboxTemplate();
//    	app.importCarverEbook();
//    	app.testEbookCarverTemplate();
//    	app.importHemingwayEbook();
//    	app.testEbookHemingwayTemplate();
//    	app.testGutenbergChekhovTemplate();
//    	app.testGutenbergMaupassantTemplate();
//    	app.testGutenbergOHenryTemplate();
//    	app.testOOPArchivesTemplate();
//    	app.testRedditTemplate();
//    	app.testTwitterJGLTemplate();
//    	app.testTwitterTwitteratureTemplate();
//    	app.testWikipediaAuthorsTemplate();
    	
//    	app.testAO3AtomStagingBatch();
//    	app.testAO3StagingBatch();
//    	app.testDNALifestyleStagingBatch();
//    	app.testDNALiteratureStagingBatch();
//    	app.testDropboxStagingBatch();
//
//    	app.testEbookCarverStagingBatch();
//
//    	app.testEbookHemingwayStagingBatch();
//    	app.testGutenbergChekhovStagingBatch();
//    	app.testGutenbergMaupassantStagingBatch();
//    	app.testGutenbergOHenryStagingBatch();
//    	app.testOOPArchivesStagingBatch();
//    	app.testRedditStagingBatch();
////    	app.testTwitterJGLStagingBatch();
////    	app.testTwitterTwitteratureStagingBatch();
//    	app.testWikipediaAuthorsStagingBatch();
    }
    
    
    @Test
    public void testAO3AtomTemplate() {
    	boolean pass = false;
    	String msg = "AO3 Atom Template success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/ao3/AO3HardyBoysAtom.json");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		t.printStackTrace();
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testAO3AtomStagingBatch() {
    	boolean pass = false;
    	String msg = "AO3 Atom Staging Batch success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromStagingBatch("AO3", "HardyBoysAtom");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		t.printStackTrace();
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testAO3Template() {
    	boolean pass = false;
    	String msg = "AO3 Template success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/ao3/AO3HardyBoys.json");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		t.printStackTrace();
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testAO3StagingBatch() {
    	boolean pass = false;
    	String msg = "AO3 Template success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromStagingBatch("AO3", "HardyBoys");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		t.printStackTrace();
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testDNALifestyleTemplate() {
    	boolean pass = false;
    	String msg = "DNA Lifestyle Template success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/dna/DNALifestyle.json");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testDNALifestyleStagingBatch() {
    	boolean pass = false;
    	String msg = "DNA Lifestyle Staging batch success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromStagingBatch("DNA", "DNALifestyle");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testDNALiteratureTemplate() {
    	boolean pass = false;
    	String msg = "DNA Literature Template success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/dna/DNALiterature.json");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testDNALiteratureStagingBatch() {
    	boolean pass = false;
    	String msg = "DNA Literature Staging Batch success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromStagingBatch("DNA", "DNALiterature");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testDropboxTemplate() {
    	boolean pass = false;
    	String msg = "Dropbox Template success";
    	try {
    		ParameterStorePropertiesFile parameterStore = new ParameterStorePropertiesFile("data", "oopcorenlp.properties");
    		
    		PostgreSQLBatchStorage pg = new PostgreSQLBatchStorage(parameterStore);
    		pg.createCorpus("submissions");
    		//CorpusBatch batch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/dropbox/OOPReading.json");
    		CorpusBatch batch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/dropbox/BatchProperties.json");
    		batch.run();
    		pass = true;
        	S3BatchStorage storage = new S3BatchStorage(parameterStore);
        	ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        	storage.storeStagingBatchJson("submissions", "OOPReading", mapper.valueToTree(batch.getData()));
        	pg.storeStagingBatchJson("submissions", "OOPReading", mapper.valueToTree(batch.getData()));
    	}
    	catch (Throwable t) {
    		t.printStackTrace();
    		logger.error(t);
    		msg = t.getMessage();
    	}

    	assertTrue(pass, msg);
    }
    
    @Test
    public void testDropboxStagingBatch() {
    	boolean pass = false;
    	String msg = "Dropbox Staging Batch success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromStagingBatch("Submissions", "OOPReading", "io.outofprintmagazine.corpus.storage.postgresql.PostgreSQLCorpora");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		t.printStackTrace();
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @BeforeAll
    public void importCarverEbook() throws Exception {
        File file = new File("C:\\Users\\rsada\\eclipse-workspace\\oopcorenlp_corpus\\corpus\\data\\Raymond Carver\\Where I'm Calling From (Vintage Cont (6)\\Where I'm Calling From (Vintage - Raymond Carver.txt.trimmed.txt");
        String buf = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
    	ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    	ScratchStorage storage = new FileScratchStorage();
    	storage.storeScratchFileString(
    			"Ebook", 
    			storage.getScratchFilePath("Carver", "Calibre", "Where_Im_Calling_From.txt"),
				buf
		);
    	
    }
    
    @Test
    public void testEbookCarverTemplate() {
    	boolean pass = false;
    	String msg = "Ebook Carver Template success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/ebook/Carver.json");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testEbookCarverStagingBatch() {
    	boolean pass = false;
    	String msg = "Ebook Carver Staging Batch success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromStagingBatch("Ebook", "Carver");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    
    @BeforeAll
    public void importHemingwayEbook() throws Exception {
        File file = new File("C:\\Users\\rsada\\eclipse-workspace\\oopcorenlp_corpus\\corpus\\data\\Ernest Hemingway\\The Complete Short Stories Of Ernest (8)\\The Complete Short Stories Of E - Ernest Hemingway.txt.trimmed.txt");
        String buf = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
    	ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    	ScratchStorage storage = new FileScratchStorage();
    	storage.storeScratchFileString(
    			"Ebook", 
    			storage.getScratchFilePath("Hemingway", "Calibre", "Complete_Short_Stories.txt"),
				buf
		);
    	
    }
    
    @Test
    public void testEbookHemingwayTemplate() {
    	boolean pass = false;
    	String msg = "Ebook Hemingway Template success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/ebook/Hemingway.json");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testEbookHemingwayStagingBatch() {
    	boolean pass = false;
    	String msg = "Ebook Hemingway Staging Batch success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromStagingBatch("Ebook", "Hemingway");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testGutenbergChekhovTemplate() {
    	boolean pass = false;
    	String msg = "Gutenberg Chekhov Template success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/gutenberg/Chekhov.json");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testGutenbergChekhovStagingBatch() {
    	boolean pass = false;
    	String msg = "Gutenberg Chekhov Staging Batch success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromStagingBatch("Gutenberg", "Chekhov");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testGutenbergMaupassantTemplate() {
    	boolean pass = false;
    	String msg = "Gutenberg Maupassant Template success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/gutenberg/Maupassant.json");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testGutenbergMaupassantStagingBatch() {
    	boolean pass = false;
    	String msg = "Gutenberg Maupassant Staging Batch success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromStagingBatch("Gutenberg", "Maupassant");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testGutenbergOHenryTemplate() {
    	boolean pass = false;
    	String msg = "Gutenberg OHenry Template success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/gutenberg/OHenry.json");
    		batch.run();
    		CorpusBatch batchTable = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/gutenberg/OHenryTableTOC.json");
    		batchTable.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testGutenbergOHenryStagingBatch() {
    	boolean pass = false;
    	String msg = "Gutenberg OHenry Staging Batch success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromStagingBatch("Gutenberg", "OHenry");
    		batch.run();
    		CorpusBatch batchTable = CorpusBatch.buildFromStagingBatch("Gutenberg", "OHenryTable");
    		batchTable.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testOOPArchivesTemplate() {
    	boolean pass = false;
    	String msg = "OOP Archives Template success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/oop/OOPArchives.json");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testOOPArchivesStagingBatch() {
    	boolean pass = false;
    	String msg = "OOP Archives Staging Batch success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromStagingBatch("Published", "OOPArchives", "io.outofprintmagazine.corpus.storage.postgresql.PostgreSQLCorpora");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testRedditTemplate() {
    	boolean pass = false;
    	String msg = "Reddit success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/reddit/Reddit.json");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testRedditStagingBatch() {
    	boolean pass = false;
    	String msg = "Reddit StagingBatch success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromStagingBatch("Reddit", "shortstories");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testTwitterJGLTemplate() {
    	boolean pass = false;
    	String msg = "Twitter JGL Template success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/twitter/TheHermeneuticCircle.json");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testTwitterJGLStagingBatch() {
    	boolean pass = false;
    	String msg = "Twitter JGL Staging Batch success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromStagingBatch("Twitter", "Hermenuetic");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testTwitterTwitteratureTemplate() {
    	boolean pass = false;
    	String msg = "Twitter Twitterature success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/twitter/Twitterature.json");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testTwitterTwitteratureStagingBatch() {
    	boolean pass = false;
    	String msg = "Twitter Twitterature Staging Batch success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromStagingBatch("Twitter", "Twitterature");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testWikipediaAuthorsTemplate() {
    	boolean pass = false;
    	String msg = "Wikipedia Authors success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromTemplate("io/outofprintmagazine/corpus/batch/impl/wikipedia/Authors.json");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    @Test
    public void testWikipediaAuthorsStagingBatch() {
    	boolean pass = false;
    	String msg = "Wikipedia Authors StagingBatch success";
    	try {
    		CorpusBatch batch = CorpusBatch.buildFromStagingBatch("Wikipedia", "Authors");
    		batch.run();
    		pass = true;
    	}
    	catch (Throwable t) {
    		logger.error(t);
    		msg = t.getMessage();
    	}
    	assertTrue(pass, msg);
    }
    
    

}
