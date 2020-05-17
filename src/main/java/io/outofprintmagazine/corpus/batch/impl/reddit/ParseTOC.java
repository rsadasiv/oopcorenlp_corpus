package io.outofprintmagazine.corpus.batch.impl.reddit;

import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
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

public class ParseTOC extends CorpusBatchStep {
	
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(ParseTOC.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
		
	public ParseTOC() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		String searchFrom = Long.toString(System.currentTimeMillis() / 1000L);
		while (true) {
			getLogger().debug(retval.size());
			getLogger().debug(searchFrom);
			if (getData().getProperties().has("maxInput") && getData().getProperties().get("maxInput").asInt() < retval.size() ) {
				break;
			}
						
			JsonNode document = httpDownloadJson(
					String.format(
							getData().getProperties().get("link").asText(),
							inputStepItem.get("subreddit").asText("shortstories"),
							searchFrom
					)
			);

			if (document.has("data") && document.get("data").isArray()) {
				ArrayNode stories = (ArrayNode) document.get("data");
				for (JsonNode story : stories) {
					searchFrom = story.get("created_utc").asText();
					if (
							story.has("selftext") 
							&& story.get("selftext").asText().trim().length() > 0 
							&& !story.get("selftext").asText().trim().equalsIgnoreCase("[removed]")
					) {
//						getLogger().debug(story.get("title").asText());
//						if (story.has("link_flair_text")) {
//							getLogger().debug(story.get("link_flair_text").asText());
//						}
//						else {
//							getLogger().debug("no flare");
//						
//						}
						if (
								!getData().getProperties().has("selector")
								|| (
										story.has("link_flair_text")
										&& story.get("link_flair_text").asText().equals(
												getData().getProperties().get("selector").asText()
												)
									)
						) {
	
							ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
							outputStepItem.put(
									"link",
									story.get("full_link").asText()
							);
		
							setAuthor(story.get("author").asText(), outputStepItem);
							setDate(story.get("created_utc").asText(), outputStepItem);
							setTitle(story.get("title").asText(), outputStepItem);
	
							try {
								setStorageLink(
									getStorage().storeScratchFileString(
										getData().getCorpusId(), 
										getOutputScratchFilePath(story.get("title").asText(), "txt"),
										story.get("selftext").asText().trim()
									),
									outputStepItem
								);
							}
							catch (IOException ioe) {
								setStorageLink(
										getStorage().storeScratchFileString(
											getData().getCorpusId(), 
											getOutputScratchFilePath(DigestUtils.md5Hex(story.get("title").asText()), "txt"),
											story.get("selftext").asText().trim()
										),
										outputStepItem
									);								
							}
							boolean alreadyExists = false;
							for (JsonNode existingOutputStepItem : retval) {
								if (existingOutputStepItem.equals(outputStepItem)) {
									alreadyExists = true;
									break;
								}
							}
							if (!alreadyExists) {
								retval.add(outputStepItem);
							}
						}
					}
				}
			}
			else {
				break;
			}
			
			if (!getData().getProperties().has("maxInput")) {
				break;
			}
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
