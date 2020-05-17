package io.outofprintmagazine.corpus.batch.impl.gutenberg;

import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;


public class ParseStory extends CorpusBatchStep {
	
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(ParseStory.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
		
	public ParseStory() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
		String rawHtml = getTextDocumentFromStorage(inputStepItem);
		rawHtml = rawHtml.replace("<br/>", "</p><p>");
		Document doc = Jsoup.parse(
				rawHtml,
				inputStepItem.get("link").asText()
		);
		boolean inStory = false;
		StringBuffer buf = new StringBuffer();
		Elements paragraphs = doc.select(
			getData().getProperties().get("selector").asText()
		);

		for (Element paragraph : paragraphs) {
			if (paragraph.selectFirst(inputStepItem.get("oop_Text").asText()) != null) {
				inStory = true;
			}	
			else if (inStory && inputStepItem.get("oop_TextNext").asText().length() > 0 && paragraph.selectFirst(inputStepItem.get("oop_TextNext").asText()) != null) {
				try {
					setStorageLink(
							getStorage().storeScratchFileString(
								inputStepItem.get("corpusId").asText(), 
								getOutputScratchFilePath(
										inputStepItem.get("stagingLinkStorage").get("objectName").asText() + "_" + inputStepItem.get("esnlc_DocTitleAnnotation").asText(), 
										"txt"
								),
								buf.toString().trim()
							),
							outputStepItem
					);
				}
				catch (IOException ioe) {
					setStorageLink(
							getStorage().storeScratchFileString(
								inputStepItem.get("corpusId").asText(), 
								getOutputScratchFilePath(
										DigestUtils.md5Hex(
												inputStepItem.get("stagingLinkStorage").get("objectName").asText() + "_" + inputStepItem.get("esnlc_DocTitleAnnotation").asText() 
										),
										"txt"
								),
								buf.toString().trim()
							),
							outputStepItem
					);
				}
				retval.add(outputStepItem);
				inStory = false;
				break;
			}
			else if (inStory) {
				buf.append(paragraph.text().trim());
				buf.append('\n');
				buf.append('\n');
			}
		}

		if (buf.length() > 0 && inStory) {
			try {
				setStorageLink(
						getStorage().storeScratchFileString(
							inputStepItem.get("corpusId").asText(), 
							getOutputScratchFilePath(
									inputStepItem.get("stagingLinkStorage").get("objectName").asText() + "_" + inputStepItem.get("esnlc_DocTitleAnnotation").asText(), 
									"txt"
							),
							buf.toString().trim()
						),
						outputStepItem
				);
			}
			catch (IOException ioe) {
				setStorageLink(
						getStorage().storeScratchFileString(
							inputStepItem.get("corpusId").asText(), 
							getOutputScratchFilePath(
									DigestUtils.md5Hex(
											inputStepItem.get("stagingLinkStorage").get("objectName").asText() + "_" + inputStepItem.get("esnlc_DocTitleAnnotation").asText() 
									),
									"txt"
							),
							buf.toString().trim()
						),
						outputStepItem
				);
			}
			retval.add(outputStepItem);
		}
		return retval;
	}
}
