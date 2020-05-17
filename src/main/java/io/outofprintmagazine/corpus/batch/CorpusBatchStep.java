package io.outofprintmagazine.corpus.batch;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.activation.MimetypesFileTypeMap;

import org.apache.logging.log4j.Logger;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.mime.MimeTypes;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.stanford.nlp.ling.CoreLabel;
import io.outofprintmagazine.corpus.batch.db.CorpusBatchStepModel;
import io.outofprintmagazine.corpus.storage.ScratchStorage;
import io.outofprintmagazine.corpus.storage.s3.S3ScratchStorage;

public abstract class CorpusBatchStep {
	
	protected abstract Logger getLogger();
	
	protected List<String> dictionaryPOS = Arrays.asList(
			"CC",
			"DT",
			"EX",
			"IN",
			"JJ",
			"JJR",
			"JJS",
			"MD",
			"NN",
			"NNS",
			"PRP",
			"PRP$",
			"RB",
			"RBR",
			"RBS",
			"RP",
			"TO",
			"UH",
			"VB",
			"VBD",
			"VBG",
			"VBN",
			"VBP",
			"VBZ",
			"WDT",
			"WP",
			"WP$",
			"WRB"
	);

	public CorpusBatchStep() {
		super();
	}
	
	private CorpusBatchStepModel data;
	
	private ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
	
	protected ObjectMapper getMapper() {
		return mapper;
	}
	
	private ScratchStorage storage = new S3ScratchStorage();
	
	public void setStorage(ScratchStorage storage) {
		this.storage = storage;
	}
		
	protected ScratchStorage getStorage() {
		return storage;
	}
	
	private SimpleDateFormat fmt = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z");
	
	protected SimpleDateFormat getDateFormat() {
		return fmt;
	}
	
	public CorpusBatchStepModel getData() {
		return data;
	}

	public void setData(CorpusBatchStepModel data) {
		this.data = data;
	}
	
	protected void copyInputToOutput(ObjectNode inputStepItem, ObjectNode outputStepItem) throws IOException {
		ObjectReader objectReader = mapper.readerForUpdating(outputStepItem);
		objectReader.readValue(inputStepItem);
	}
	
	protected ObjectNode copyInputToOutput(ObjectNode inputStepItem) throws IOException {
		ObjectNode outputStepItem = mapper.createObjectNode();
		copyInputToOutput(inputStepItem, outputStepItem);
		return outputStepItem;

	}
	
	/*
	 * for each ObjectNode inputItem in input
	 * if inputItem not in Input
	 * try {
	 * 	getOutput().addAll(runOne(inputItem))
	 * 	getInput().add(inputItem)
	 * }
	 * return getOutput()
	 */
	
	public ArrayNode run(ArrayNode input) {
		int count = 0;
		for (JsonNode inputItem : input) {
			if (getData().getProperties().has("maxInput") && getData().getProperties().get("maxInput").asInt() < count ) {
				break;
			}
			count++;
			boolean foundInputItem = false;

			if (!(getData().getProperties().has("noCache") && getData().getProperties().get("noCache").asBoolean())) {
				for (JsonNode existingInputItem : getData().getInput()) {
					if (existingInputItem.equals(inputItem)) {
						foundInputItem = true;
						break;
					}
				}
			}
			if (!foundInputItem) {
				try {
					ArrayNode generatedOutput = runOne((ObjectNode)inputItem);
					if (!(getData().getProperties().has("noCache") && getData().getProperties().get("noCache").asBoolean())) {
						getData().getInput().add(inputItem);
					}
					
					for (JsonNode generatedOutputItem : generatedOutput) {
						boolean foundOutputItem = false;
						for (JsonNode existingOutputItem : getData().getInput()) {
							if (existingOutputItem.equals(generatedOutputItem)) {
								foundOutputItem = true;
								break;
							}
						}
						if (!foundOutputItem) {
							getData().getOutput().add(generatedOutputItem);
						}
					}
				}
				catch (Throwable t) {
					t.printStackTrace();
					getLogger().error(t);
				}
			}
		}
		return getData().getOutput();
	}
	
	public abstract ArrayNode runOne(ObjectNode input) throws Exception;
	
	public void configure(ObjectNode properties) {
		getData().setProperties(properties);
	}
	
//	protected String getText(Document doc) {
//		StringBuffer buf = new StringBuffer();
//		Elements paragraphs = doc.select(getData().getProperties().get("oop_Text").asText());
//		for (Element paragraph : paragraphs) {
//			buf.append(paragraph.wholeText().trim());
//			buf.append('\n');
//			buf.append('\n');
//		}
//		return buf.toString();
//	}
	
	protected String getText(Element element) {
		StringBuffer buf = new StringBuffer();
		Elements paragraphs = element.select(getData().getProperties().get("oop_Text").asText());
		for (Element paragraph : paragraphs) {
			buf.append(paragraph.wholeText().trim());
			buf.append('\n');
			buf.append('\n');
		}
		return buf.toString();
	}

	protected String getText(ObjectNode outputStepItem) {
		return outputStepItem.get(
				"oop_Text"
		).asText();
	}
	
	protected String getAuthor(Document doc) {
		return 
				doc.select(
					getData().getProperties().get("esnlc_AuthorAnnotation").asText()
				).attr("content");
	}
	
	protected String getAuthor(ObjectNode outputStepItem) {
		return outputStepItem.get(
				"esnlc_AuthorAnnotation"
		).asText();
	}
	
	protected void setAuthor(String author, ObjectNode outputStepItem) {
		outputStepItem.put(
				"esnlc_AuthorAnnotation", 
				author
		);
	}
	
	protected void setAuthor(Document doc, ObjectNode outputStepItem) {
		setAuthor(getAuthor(doc), outputStepItem);
	}
	
	protected String getTitle(Document doc) {
		return
				doc.select(
						getData().getProperties().get("esnlc_DocTitleAnnotation").asText()
				).text();
	}
	
	protected String getTitle(ObjectNode outputStepItem) {
		return outputStepItem.get(
				"esnlc_DocTitleAnnotation"
		).asText();
	}
	
	protected void setTitle(String title, ObjectNode outputStepItem) {
		outputStepItem.put(
				"esnlc_DocTitleAnnotation", 
				title
		);
	}
	
	protected void setTitle(Document doc, ObjectNode outputStepItem) {
		setTitle(getTitle(doc), outputStepItem);
	}
	
	protected String getThumbnail(Document doc) {
		return 
				doc.select(
						getData().getProperties().get("oop_DocThumbnail").asText()
				).attr("content");
	}
	
	protected void setThumbnail(String thumbnail, ObjectNode outputStepItem) {
		outputStepItem.put(
				"oop_DocThumbnail", 
				thumbnail
		);
	}
	
	protected void setThumbnail(Document doc, ObjectNode outputStepItem) {
		setThumbnail(getThumbnail(doc), outputStepItem);
	}
	
	protected String getDate(Document doc) {
		return 
				doc.select(
						getData().getProperties().get("esnlc_DocDateAnnotation").asText()
				).attr("content");
	}
	
	protected String getDate(ObjectNode outputStepItem) {
		return outputStepItem.get(
				"esnlc_DocDateAnnotation"
		).asText();
	}
	
	protected void setDate(String date, ObjectNode outputStepItem) {
		outputStepItem.put(
				"esnlc_DocDateAnnotation", 
				date
		);
	}
					
	protected void setDate(Document doc, ObjectNode outputStepItem) {
		setDate(getDate(doc), outputStepItem);
	}
	
	protected void setDate(ObjectNode outputStepItem) {
		setDate(getDateFormat().format(new Date(System.currentTimeMillis())), outputStepItem);
	}
	
	protected void setLink(String link, ObjectNode outputStepItem) {
		outputStepItem.put(
				"link",
				link
		);
	}
	
	protected String getLink(ObjectNode outputStepItem) {
		return outputStepItem.get("link").asText();
	}
	
	protected void setStorageLink(String storage, ObjectNode outputStepItem) {
		outputStepItem.put(
				"stagingLinkStorage",
				storage
		);
	}
	
	protected void setDocID(ObjectNode outputStepItem, String docID) {
		outputStepItem.put("esnlc_DocIDAnnotation", docID);
	}
	
	protected String getDocID(ObjectNode outputStepItem) {
		return outputStepItem.get("esnlc_DocIDAnnotation").asText();
	}
	
	protected String getStorageLink(ObjectNode outputStepItem) {
		return outputStepItem.get("stagingLinkStorage").asText();
	}
	
//	protected ObjectNode getPlainTextStorageProperties() {
//		ObjectNode storageProperties = mapper.createObjectNode();
//		//"Sun, 16 Feb 2020 23:17:38 GMT"
//		storageProperties.put("Content-Type", "text/plain");
//		storageProperties.put("mimeType", "text/plain");
//		storageProperties.put("charset",  "UTF-8");
//		storageProperties.put("Date", fmt.format(new Date(System.currentTimeMillis())));
//		
//		return storageProperties;
//	}
	
	protected Document getJsoupDocumentFromStorage(ObjectNode inputStepItem) throws Exception {
		return Jsoup.parse(
				getStorage().getScratchFileStream(
						getData().getCorpusId(),
						getStorageLink(inputStepItem)
				), 
				"utf-8",
				inputStepItem.get("link").asText()
		);
	}
	
	protected String getTextDocumentFromStorage(ObjectNode inputStepItem) throws Exception {
		return getStorage().getScratchFileString(
				getData().getCorpusId(),
				getStorageLink(inputStepItem)
		);
	}
	
	protected String getTextDocumentFromStorage(ObjectNode inputStepItem, String property) throws Exception {
		return getStorage().getScratchFileString(
				getData().getCorpusId(),
				inputStepItem.get(property).asText()
		);
	}
	
	protected JsonNode getJsonNodeFromStorage(ObjectNode inputStepItem) throws Exception {
		return getMapper().readTree(
				getStorage().getScratchFileStream(
						getData().getCorpusId(),
						getStorageLink(inputStepItem)
				)
		);
	}
	
	protected JsonNode getJsonNodeFromStorage(ObjectNode inputStepItem, String property) throws Exception {
		return getMapper().readTree(
				getStorage().getScratchFileStream(
						getData().getCorpusId(),
						inputStepItem.get(property).asText()
				)
		);
	}
	
//	protected JsonNode getStagedStorageProperties(ObjectNode inputStepItem) throws Exception {
//		return getMapper().readTree(
//				storage.getScratchFilePropertiesStream(
//						getData().getCorpusId(),
//						getStorageLink(inputStepItem)
//				)
//		);
//	}
	
	public String getOutputScratchFilePathFromInput(ObjectNode inputStepItem, String extension) throws Exception {
		String fileName = UUID.randomUUID().toString();
		if (inputStepItem.has("esnlc_DocIDAnnotation")) {
			fileName = inputStepItem.get("esnlc_DocIDAnnotation").asText();
		}
		else if (inputStepItem.has("stagingLinkStorage")) {
			fileName = 
					getStorage().trimFileExtension(
							getStorage().getFileNameFromPath(
									getStorageLink(inputStepItem)
							)
					);
		}
		else if (inputStepItem.has("link")) {
			fileName = URLEncoder.encode(inputStepItem.get("link").asText(), "utf-8");
		}
		return getOutputScratchFilePath(fileName, extension);
		
	}
	
	public String getOutputScratchFilePath(String fileName) throws Exception {
		return getStorage().getScratchFilePath(
				getData().getCorpusBatchId(),
				String.format(
						"%s-%s",
						getData().getCorpusBatchStepSequenceId().toString(), 
						getData().getCorpusBatchStepId()
				),
				fileName
		);
	}
	
	public String getOutputScratchFilePath(String fileName, String extension) throws Exception {
		return getStorage().getScratchFilePath(
				getData().getCorpusBatchId(),
				String.format(
						"%s-%s",
						getData().getCorpusBatchStepSequenceId().toString(), 
						getData().getCorpusBatchStepId()
				),
				String.format("%s.%s",
						fileName, 
						extension
				)
		);
	}
	
	protected String getMimeTypeFromExtension(String extension) {
		File file = new File("tmp."+extension);
		MimetypesFileTypeMap fileTypeMap = new MimetypesFileTypeMap(this.getClass().getClassLoader().getResourceAsStream("mime.types"));
		return fileTypeMap.getContentType(file.getName());
	}
	
	protected String getExtensionFromMimeType(String mimeType) throws MimeTypeException {
		MimeTypes allTypes = MimeTypes.getDefaultMimeTypes();
		MimeType tmp = allTypes.forName(mimeType);
		return tmp.getExtension();
	}
	
	protected boolean isDictionaryWord(String pos) {
		if (pos.equals("NNP") || pos.equals("NNPS")) {
			return true;
		}
		else {
			return dictionaryPOS.contains(pos);
		}
	}

}
