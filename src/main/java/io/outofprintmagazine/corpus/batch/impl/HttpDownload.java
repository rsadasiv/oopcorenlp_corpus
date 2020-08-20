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
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;

public class HttpDownload extends CorpusBatchStep {
	
	private static final Logger logger = LogManager.getLogger(HttpDownload.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}

	public HttpDownload() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		int sleepBackoff = ThreadLocalRandom.current().nextInt(10000, 20000);
		Thread.sleep(sleepBackoff);
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = getMapper().createObjectNode();
		ObjectReader objectReader = getMapper().readerForUpdating(outputStepItem);
		objectReader.readValue(inputStepItem);
		ObjectNode storageProperties = getMapper().createObjectNode();

			String linkContent = httpDownload(
					inputStepItem.get("link").asText(), 
					storageProperties
			);
			
			setDate(
					storageProperties.get("Last-Modified").asText(
						getDateFormat().format(
								new Date(System.currentTimeMillis())
						)
					), 
					outputStepItem
			);

			try {
				setStorageLink(
					getStorage().storeScratchFileString(
						getData().getCorpusId(),
						getOutputScratchFilePathFromInput(
								inputStepItem,
								getExtensionFromMimeType(storageProperties.get("mimeType").asText("html"))
						),
						linkContent
					),
					outputStepItem
				);
			}
			catch (IOException ioe) {
				setStorageLink(
						getStorage().storeScratchFileString(
							getData().getCorpusId(),
							getStorage().getScratchFilePath(
									getData().getCorpusBatchId(),
									String.format(
											"%s-%s",
											getData().getCorpusBatchStepSequenceId().toString(), 
											getData().getCorpusBatchStepId()
									),
									String.format("%s.%s",
											DigestUtils.md5Hex(inputStepItem.get("link").asText()).toUpperCase(), 
											"html"
									)
							),
							linkContent
						),
						outputStepItem
					);
			}
			retval.add(outputStepItem);
			return retval;
	}
	
	protected String httpDownload(String url, ObjectNode properties) throws IOException {
		String responseBody = null;
        CloseableHttpClient httpclient = getHttpClient();
        try {
        	HttpGet http = new HttpGet(url);
            http.setHeader("user-agent", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1");
            http.setHeader("accept-language", "en-US;q=0.9,en;q=0.8");
            responseBody = httpclient.execute(
            		http, 
            		new PropertiesResponseHandler(properties)
            );
        }
        finally {
            httpclient.close();
        }
        return responseBody;
	}
	
	
	protected CloseableHttpClient getHttpClient() {
		return HttpClients.custom()
                .setServiceUnavailableRetryStrategy(
                		new ServiceUnavailableRetryStrategy() {
                			public boolean retryRequest(
                					final HttpResponse response, final int executionCount, final HttpContext context) {
                					int statusCode = response.getStatusLine().getStatusCode();
                					return (statusCode == 503 || statusCode == 500 || statusCode == 429) && executionCount < 5;
                			}

                			public long getRetryInterval() {
                				return 10;
                			}
                		})
                .setRedirectStrategy(new LaxRedirectStrategy())
                .build();
	}
	
	class PropertiesResponseHandler implements ResponseHandler<String> {

		ObjectNode properties;
		
		public PropertiesResponseHandler(ObjectNode properties) {
			super();
			this.properties = properties;
		}
		
		public String handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
			int status = response.getStatusLine().getStatusCode();
            if (status >= 200 && status < 300) {
                HttpEntity entity = response.getEntity();
                for (Header header : response.getAllHeaders()) {
                	if (!properties.has(header.getName())) {
                		properties.put(header.getName(), header.getValue());
                	}
                	else {
                		JsonNode existingNode = properties.get(header.getName());
                		if (existingNode.isArray()) {
                			ArrayNode aExistingNode = (ArrayNode) existingNode;
                			aExistingNode.add(header.getValue());
                		}
                		else {
                			java.lang.String existingValue = existingNode.asText();
                			ArrayNode aExistingNode = properties.putArray(header.getName());
                			aExistingNode.add(existingValue);
                			aExistingNode.add(header.getValue());
                		}
                	}
                }
                ContentType contentType = ContentType.getOrDefault(entity);
                if (contentType != null && contentType.getMimeType() != null) {
                	properties.put("mimeType", contentType.getMimeType());
                }
                if (contentType != null && contentType.getCharset() != null) {               
                	properties.put("charset", contentType.getCharset().name());
                }
                else {
                	properties.put("charset", "UTF-8");
                }
                return EntityUtils.toString(entity, properties.get("charset").asText("UTF-8"));
            } 
            else {
                throw new ClientProtocolException("Unexpected response status: " + status);
            }
		}
		
	}
	
	




}
