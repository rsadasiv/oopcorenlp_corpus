package io.outofprintmagazine.corpus.batch.impl.oop;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

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
		Document doc = getJsoupDocumentFromStorage(inputStepItem);
			
		String issueThumbnailUrl = getThumbnail(doc);
		if (issueThumbnailUrl == null) {
			issueThumbnailUrl = getThumbnailLookup(doc);
		}
		
		Elements storylinks = doc.select("area");
		if (storylinks.isEmpty()) {
			storylinks = doc.select("a");
		}
		for (Element storylink : storylinks) {
			String storyHref = storylink.attr("href").trim();
			if (!storyHref.startsWith(".") && !storyHref.startsWith("index.html")) {
				if (!storyHref.equals("editors-note.html")) {
					ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
					outputStepItem.remove("stagingLinkStorage");
					
					if (inputStepItem.get("issueBaseHref").asText().equals("https://www.outofprintmagazine.co.in/archive/june_2011_issue/")) {
						Document landingPage = Jsoup.connect(inputStepItem.get("issueBaseHref").asText() + storyHref).get();
						Element imgLink = landingPage.selectFirst("p.writersintro>a[href]>img[src]");

						if (imgLink != null) {
							setLink(inputStepItem.get("issueBaseHref").asText() + imgLink.parent().attr("href").trim(), outputStepItem);
							setThumbnail(inputStepItem.get("issueBaseHref").asText() + imgLink.attr("src").trim(), outputStepItem);
						}
						else {
							setThumbnail(issueThumbnailUrl, outputStepItem);
						}
					}
					else {

						setLink(inputStepItem.get("issueBaseHref").asText() + storyHref, outputStepItem);
						setThumbnail(issueThumbnailUrl, outputStepItem);
					}
					retval.add(outputStepItem);
				}
			}
		}
		return retval;
	}

	protected String getThumbnailLookup(Document doc) {
		String retval = "";
		//https://www.outofprintmagazine.co.in/archive/sept-2015-issue/index.html
		//https://www.outofprintmagazine.co.in/archive/sept-2015-issue/images/cover_pic-sept_2015.jpg

		if (getData().getProperties().get("issueBaseHref").asText().equals("https://www.outofprintmagazine.co.in/archive/sept_2014_issue/")) {
			return getData().getProperties().get("issueBaseHref").asText() + "images/cover_pic-september_2014.jpg";
		}
		else if (getData().getProperties().get("issueBaseHref").asText().equals("https://www.outofprintmagazine.co.in/archive/dec_2014_issue/")) {
			return getData().getProperties().get("issueBaseHref").asText() + "images/cover_pic-december_2014.jpg";
		}
		else if (getData().getProperties().get("issueBaseHref").asText().equals("https://www.outofprintmagazine.co.in/archive/dec_2013_issue/")) {
			return getData().getProperties().get("issueBaseHref").asText() + "images/cover_pic-december_2013.jpg";
		}
		else if (getData().getProperties().get("issueBaseHref").asText().equals("https://www.outofprintmagazine.co.in/archive/sept_2013_issue/")) {
			return getData().getProperties().get("issueBaseHref").asText() + "images/cover_pic-september_2013.jpg";
		}
		else if (getData().getProperties().get("issueBaseHref").asText().equals("https://www.outofprintmagazine.co.in/archive/june-2013-issue/")) {
			return getData().getProperties().get("issueBaseHref").asText() + "images/cover-pic-june_2013.jpg";
		}
		else if (getData().getProperties().get("issueBaseHref").asText().equals("https://www.outofprintmagazine.co.in/archive/march-2013-issue/")) {
			return getData().getProperties().get("issueBaseHref").asText() + "images/cover-pic-march_2013.jpg";
		}
		else if (getData().getProperties().get("issueBaseHref").asText().equals("https://www.outofprintmagazine.co.in/archive/sept_2012_issue/")) {
			return getData().getProperties().get("issueBaseHref").asText() + "images/cover-pic-september_2012.jpg";
		}
		else if (getData().getProperties().get("issueBaseHref").asText().equals("https://www.outofprintmagazine.co.in/archive/june_2012_issue/")) {
			return getData().getProperties().get("issueBaseHref").asText() + "images/cover-pic-july-2012.jpg";
		}
		else if (getData().getProperties().get("issueBaseHref").asText().equals("https://www.outofprintmagazine.co.in/archive/march-2012-issue/")) {
			return getData().getProperties().get("issueBaseHref").asText() + "images/cover_pic_march_2012.jpg";
		}
		else if (getData().getProperties().get("issueBaseHref").asText().equals("https://www.outofprintmagazine.co.in/archive/dec_2011_issue/")) {
			return getData().getProperties().get("issueBaseHref").asText() + "images/cover_pic_december_2011.jpg";
		}
		else if (getData().getProperties().get("issueBaseHref").asText().equals("https://www.outofprintmagazine.co.in/archive/sept_2011_issue/")) {
			return getData().getProperties().get("issueBaseHref").asText() + "images/cover_pic_sept_2011.jpg";
		}
		else if (getData().getProperties().get("issueBaseHref").asText().equals("https://www.outofprintmagazine.co.in/archive/june_2011_issue/")) {
			return getData().getProperties().get("issueBaseHref").asText() + "images/cover_pic_june_2011.jpg";
		}
		else if (getData().getProperties().get("issueBaseHref").asText().equals("https://www.outofprintmagazine.co.in/archive/mar_2011_issue/")) {
			return getData().getProperties().get("issueBaseHref").asText() + "images/cover_pic_mar_2011.jpg";
		}
		else if (getData().getProperties().get("issueBaseHref").asText().equals("https://www.outofprintmagazine.co.in/archive/dec_2010_issue/")) {
			return getData().getProperties().get("issueBaseHref").asText() + "images/cover_pic_dec_2010.jpg";
		}
		else if (getData().getProperties().get("issueBaseHref").asText().equals("https://www.outofprintmagazine.co.in/archive/sept_2010_issue/")) {
			return getData().getProperties().get("issueBaseHref").asText() + "images/cover_pic_cclare_arni.jpg";
		}
		else if (getData().getProperties().get("issueBaseHref").asText().equals("https://www.outofprintmagazine.co.in/archive/first_issue/")) {
			return getData().getProperties().get("issueBaseHref").asText() + "images/home_main_image.jpg";
		}

		String pattern = "https://www.outofprintmagazine.co.in/archive/(.*)-issue";
		Pattern r = Pattern.compile(pattern);
		Matcher m = r.matcher(getData().getProperties().get("link").asText());
		if (m.find()) {
			retval = getData().getProperties().get("issueBaseHref").asText() + 
					"images/cover_pic-" + m.group(1).replace("-", "_") +".jpg";
		}
		else {
			pattern = "https://www.outofprintmagazine.co.in/archive/(.*)_issue";
			r = Pattern.compile(pattern);
			m = r.matcher(getData().getProperties().get("link").asText());
			if (m.find()) {
				retval = getData().getProperties().get("issueBaseHref").asText() + 
						"images/cover_pic-" + m.group(1) +".jpg";
			}				
		}


		return retval;
	}

}
