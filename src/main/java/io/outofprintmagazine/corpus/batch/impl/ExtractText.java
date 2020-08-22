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
package io.outofprintmagazine.corpus.batch.impl;

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
import io.outofprintmagazine.corpus.batch.ICorpusBatchStep;

public class ExtractText extends CorpusBatchStep implements ICorpusBatchStep {

	private static final Logger logger = LogManager.getLogger(ExtractText.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
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
	
	public ExtractText() {
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
