package io.outofprintmagazine.corpus.batch.impl.dropbox;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dropbox.core.DbxException;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.ListFolderErrorException;
import com.dropbox.core.v2.files.ListFolderResult;
import com.dropbox.core.v2.files.Metadata;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;

public class ParseArchive extends CorpusBatchStep {
	
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(ParseArchive.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
	
	private DbxClientV2 client = null;
	
	public ParseArchive() throws IOException {
		super();
		InputStream input = new FileInputStream("data/dropbox.properties");
        Properties props = new Properties();
        props.load(input);
        input.close();
		DbxRequestConfig config = DbxRequestConfig.newBuilder("dropbox/OOP_Nlp").build();
        client = new DbxClientV2(config, props.getProperty("accessToken"));
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws IOException {
		ArrayNode retval = getMapper().createArrayNode();
		try {
			ListFolderResult directories = client.files().listFolder(inputStepItem.get("link").asText());
	        while (true) {
	            for (Metadata directory : directories.getEntries()) {
	            	//does this directory start with "Potential Stories "
	            	if (directory.getName().startsWith(getData().getProperties().get("issueNumber").asText())) {
	            		//logger.debug(directory.getPathDisplay());
		            	//does this have a subdirectory named Stories
		            	ListFolderResult subDirectories = client.files().listFolder(directory.getPathDisplay());
				        while (true) {
				            for (Metadata subDirectory : subDirectories.getEntries()) {
				            	if (subDirectory.getName().startsWith(getData().getProperties().get("subdirectoryName").asText())) {
				            		//logger.debug(subDirectory.getPathDisplay());
									try {
										ListFolderResult stories = client.files().listFolder(subDirectory.getPathDisplay());
										/*
										 * author.title.docx
										 * author.title.mod.docx
										 * author.title.translator.docx
										 * author.title.translator.mod.docx
										 * author.title.trans.translator.docx
										 * author - title.docx
										 * author - title.mod.docx
										 * authorfirst.authorlast.title.docx
										 * authorfirst
										 */
								        while (true) {
								            for (Metadata story : stories.getEntries()) {
								            	if (!(
								            			story.getName().toLowerCase().contains(".mod") || 
								            			story.getName().toLowerCase().contains(" mod") ||
								            			story.getName().toLowerCase().contains(".final") ||
								            			story.getName().toLowerCase().contains("revised") ||
								            			story.getName().toLowerCase().contains("author.") ||
								            			story.getName().toLowerCase().contains("edits.") ||
								            			story.getName().contains("_NA.")
								            			
								            		)) {
								            		//logger.debug(story.getPathDisplay());
								            		String[] items = story.getName().split("\\.");
								            		if (items.length > 1) {
														ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
														outputStepItem.put("issueNumber", 
																directory.getName().substring(
																		getData().getProperties().get("issueNumber").asText().length()
																)
														);
														
														if (items.length == 2) {
															String[] dashItems = items[0].split("-");
															if (dashItems.length == 2) {
																outputStepItem.put(
																		"esnlc_DocTitleAnnotation", 
																		dashItems[1].trim()
																);
																outputStepItem.put(
																		"esnlc_AuthorAnnotation", 
																		dashItems[0].trim()
																);											
															}
															else {
																outputStepItem.put(
																		"esnlc_DocTitleAnnotation", 
																		items[items.length-2].trim()
																);
															}
														}
														else if (items.length == 3) {
															if (items[2].toLowerCase().startsWith("trans")) {
																outputStepItem.put(
																		"esnlc_DocTitleAnnotation", 
																		items[1].trim()
																);
																outputStepItem.put(
																		"esnlc_AuthorAnnotation", 
																		items[0].trim()
																);	
															}
															else {
																outputStepItem.put(
																		"esnlc_DocTitleAnnotation", 
																		items[items.length-2].trim()
																);
																outputStepItem.put(
																		"esnlc_AuthorAnnotation", 
																		items[items.length-3].trim()
																);
															}
														}
														else {
															if (items[2].toLowerCase().startsWith("trans")) {
																outputStepItem.put(
																		"esnlc_DocTitleAnnotation", 
																		items[1].trim()
																);
																outputStepItem.put(
																		"esnlc_AuthorAnnotation", 
																		items[0].trim()
																);	
															}
															else {
																outputStepItem.put(
																		"esnlc_DocTitleAnnotation", 
																		items[items.length-2].trim()
																);
																StringBuffer buf = new StringBuffer();
																for (int i=0;i<items.length-2;i++) {
																	buf.append(items[i]);
																	buf.append(" ");
																}
																outputStepItem.put(
																		"esnlc_AuthorAnnotation", 
																		buf.toString().trim()
																);
															}
														}
														outputStepItem.put("link", story.getPathDisplay());
														retval.add(outputStepItem);

								            		}
								            	}
							
								            }
							
								            if (!stories.getHasMore()) {
								                break;
								            }
							
								            stories = client.files().listFolderContinue(stories.getCursor());
								        }
									}
									catch (ListFolderErrorException e) {
										logger.error(e);
										throw new IOException(e);
									}
														            		
				            	}
				            }
				            if (!subDirectories.getHasMore()) {
				                break;
				            }

				            subDirectories = client.files().listFolderContinue(subDirectories.getCursor());
				        }
	            	}
	            }

	            if (!directories.getHasMore()) {
	                break;
	            }

	            directories = client.files().listFolderContinue(directories.getCursor());
	        }
	        return retval;
		} 
		catch (ListFolderErrorException e) {
			logger.error(e);
			throw new IOException(e);
		} 
		catch (DbxException e) {
			logger.error(e);
			throw new IOException(e);
		}
	}
}
