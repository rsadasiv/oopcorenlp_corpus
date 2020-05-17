package io.outofprintmagazine.corpus.batch.impl.dropbox;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dropbox.core.DbxDownloader;
import com.dropbox.core.DbxException;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.FileMetadata;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;


public class DropboxDownload extends CorpusBatchStep {
	
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(DropboxDownload.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
	
	private DbxClientV2 client = null;	
	
	public DropboxDownload() throws IOException {
		super();
		InputStream input = new FileInputStream("data/dropbox.properties");
        Properties props = new Properties();
        props.load(input);
        input.close();
		DbxRequestConfig config = DbxRequestConfig.newBuilder("dropbox/OOP_Nlp").build();
        client = new DbxClientV2(config, props.getProperty("accessToken"));		
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		try {
			ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
			DbxDownloader<FileMetadata> downloader = client.files().download(inputStepItem.get("link").asText());
			setDate(getDateFormat().format(downloader.getResult().getClientModified()), outputStepItem);
			String[] paths = inputStepItem.get("link").asText().split("/");
			setStorageLink(
					getStorage().storeScratchFileStream(
							getData().getCorpusId(),
							getOutputScratchFilePath(paths[paths.length-1]),
							downloader.getInputStream()
					), 
					outputStepItem
			);
			setThumbnail("https://upload.wikimedia.org/wikipedia/commons/thumb/7/78/Dropbox_Icon.svg/500px-Dropbox_Icon.svg.png", outputStepItem);
			retval.add(outputStepItem);
			return retval;
		}
		catch (DbxException e) {
			logger.error(e);
			throw new IOException(e);
		}
	}
}
