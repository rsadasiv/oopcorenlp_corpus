package io.outofprintmagazine.corpus.batch.impl.dropbox;

import java.io.IOException;
import java.io.InputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.microsoft.OfficeParserConfig;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

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
	
	protected String parseToString(InputStream stream) throws IOException, SAXException, TikaException {
		ParseContext parseContext = new ParseContext();
	    AutoDetectParser parser = new AutoDetectParser();
	    BodyContentHandler handler = new BodyContentHandler();
	    Metadata metadata = new Metadata();
	    OfficeParserConfig officeParserConfig = new OfficeParserConfig();
	    officeParserConfig.setIncludeHeadersAndFooters(false);
	    officeParserConfig.setIncludeDeletedContent(false);
	    officeParserConfig.setIncludeMoveFromContent(false);
	    parseContext.set(OfficeParserConfig.class, officeParserConfig);
	    parser.parse(stream, handler, metadata, parseContext);	    
	    return handler.toString();
	}
	
	public ParseStory() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		InputStream fis = null;
		String text = null;
	    try {
	    	fis = getStorage().getScratchFileStream(
					getData().getCorpusId(),
					getStorageLink(inputStepItem)
			);
	    	text = parseToString(fis);
	    } 
	    catch (TikaException e) {
			throw new IOException(e);
		} 
	    catch (SAXException e) {
	    	throw new IOException(e);
		}
	    finally {
	    	if (fis != null) {
	    		fis.close();
	    		fis = null;
	    	}
	    }
		if (text != null) {
			ObjectNode outputStepItem = copyInputToOutput(inputStepItem);

			setStorageLink(
					getStorage().storeScratchFileString(
						getData().getCorpusId(),
						getOutputScratchFilePathFromInput(inputStepItem, "txt"),
						text.trim()
					),
					outputStepItem
			);
			retval.add(outputStepItem);
		}
		return retval;
	}
}
