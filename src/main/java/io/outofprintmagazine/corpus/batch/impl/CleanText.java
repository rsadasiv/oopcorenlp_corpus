package io.outofprintmagazine.corpus.batch.impl;

import java.io.IOException;
import java.io.StringReader;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.stanford.nlp.util.StringUtils;
import io.outofprintmagazine.corpus.batch.CorpusBatchStep;

public class CleanText extends CorpusBatchStep {
	
	private static final Logger logger = LogManager.getLogger(CleanText.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
	
	//need to convert single angle quote to double vertical quote
	//if the single quote starts the line - yes
	// ^\u2018
	//if the single quote ends the line - yes
	// \u2019$
	//if the single quote has printing characters on both sides - no
	//else - yes
	// \s\u2018
	// \u2019\s
	//
	//still having a problem with plural possessive. The Smiths' house.
	Pattern startLine = Pattern.compile("^\\u2018");
	Pattern startWord = Pattern.compile("\\s\\u2018(\\S)");
	Pattern endLine = Pattern.compile("\\u2019$");
	Pattern endWord = Pattern.compile("(\\S)\\u2019\\s");
	Pattern endSentence = Pattern.compile("\\u2019(\\.)");
	
	public CleanText() {
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		String doc = getTextDocumentFromStorage(inputStepItem);
		
		ObjectNode outputStepItem = getMapper().createObjectNode();
		ObjectReader objectReader = getMapper().readerForUpdating(outputStepItem);
		objectReader.readValue(inputStepItem);
		ObjectNode storageProperties = getMapper().createObjectNode();
		//"Sun, 16 Feb 2020 23:17:38 GMT"
		storageProperties.put("Content-Type", "text/plain");
		storageProperties.put("mimeType", "text/plain");
		storageProperties.put("charset",  "us-ascii");
		storageProperties.put("Date", getDateFormat().format(new Date(System.currentTimeMillis())));

		setStorageLink(
				getStorage().storeScratchFileString(
						getData().getCorpusId(),
						getOutputScratchFilePathFromInput(inputStepItem, "txt"),
						processPlainUnicode(doc)
				), 
				outputStepItem
		);
		retval.add(outputStepItem);
		return retval;
	}
	
	@Override
	public void configure(ObjectNode properties) {
		getData().setProperties(properties);
	}
	

	private String processPlainUnicode(String input) throws IOException {
		StringBuffer output = new StringBuffer();
		for (String line : IOUtils.readLines(new StringReader(input))) {
			line = line.replaceAll("[\\u00A0\\u2007\\u202F]+", " ").trim();
			line = line.replaceAll("[\\u2028]", "/n").trim();
			if (line.length() > 0) {
				line = startLine.matcher(line).replaceAll("\"");
				line = startWord.matcher(line).replaceAll(" \"$1");
				line = endLine.matcher(line).replaceAll("\"");
				line = endWord.matcher(line).replaceAll("$1\" ");
				line = endSentence.matcher(line).replaceAll("\"$1");
				line = StringUtils.toAscii(StringUtils.normalize(line));
				output.append(line);
				output.append('\n');
				output.append('\n');
			}
		}
		return output.toString();

	}
}
