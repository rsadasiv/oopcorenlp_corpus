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
package io.outofprintmagazine.corpus.batch.impl.reddit;

import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.protocol.HttpContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;



public class ParseAuthorThumbnail extends CorpusBatchStep {
	
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(ParseAuthorThumbnail.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
			
	public ParseAuthorThumbnail() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws IOException {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
		setThumbnail(getThumbnail(outputStepItem.get("esnlc_AuthorAnnotation").asText()), outputStepItem);
		retval.add(outputStepItem);
		return retval;
	}
				
	protected String getThumbnail(String author) throws MalformedURLException, IOException {
		String retval = "https://www.redditinc.com/assets/images/site/reddit-logo.png";
		try {
			if (!author.equals("[deleted]")) {
				JsonNode authorPage = httpDownloadJson(
						String.format(
								getData().getProperties().get("link").asText("https://www.reddit.com/user/%s/about.json"), 
								author
						)
				);
				if (
						authorPage.has("data") 
						&& authorPage.get("data").has("icon_img") 
						&& !authorPage.get("data").get("icon_img").asText().startsWith("https://styles.redditmedia.com")
				) {
					retval = authorPage.get("data").get("icon_img").asText();
				}
			}
		}
		catch (Exception e) {
			getLogger().error(e);
		}
		return retval;
		
	}
	
	protected JsonNode httpDownloadJson(String url) throws IOException {
		JsonNode retval = null;
        CloseableHttpClient httpclient = getHttpClient();
        try {
        	CloseableHttpResponse response = httpclient.execute(new HttpGet(url));
            retval = getMapper().readTree(response.getEntity().getContent());
        }
        finally {
            httpclient.close();
        }
        return retval;
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

}
