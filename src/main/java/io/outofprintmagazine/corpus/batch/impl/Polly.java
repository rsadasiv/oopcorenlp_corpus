package io.outofprintmagazine.corpus.batch.impl;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.polly.AmazonPolly;
import com.amazonaws.services.polly.AmazonPollyClientBuilder;
import com.amazonaws.services.polly.model.DescribeVoicesRequest;
import com.amazonaws.services.polly.model.DescribeVoicesResult;
import com.amazonaws.services.polly.model.StartSpeechSynthesisTaskRequest;
import com.amazonaws.services.polly.model.StartSpeechSynthesisTaskResult;
import com.amazonaws.services.polly.model.Voice;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.storage.s3.AwsUtils;
import io.outofprintmagazine.corpus.storage.s3.S3BatchStorage;

public class Polly extends CorpusBatchStep {

	
	private static final Logger logger = LogManager.getLogger(CoreNLP.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
	private AmazonPolly polly;
	private Voice voice;
	private String defaultPath = "";
	private String defaultBucket;
	
	public Polly() throws IOException {
		super();
		polly = AmazonPollyClientBuilder.standard()
			      .withCredentials(new AWSStaticCredentialsProvider(AwsUtils.getInstance().getBasicCredentials()))
			      .withRegion(AwsUtils.getInstance().getRegion())
			      .build();
		S3BatchStorage b = new S3BatchStorage();
		defaultPath = b.getDefaultPath() + "/";
		defaultBucket = S3BatchStorage.defaultBucket;
	}

	@Override
	public ArrayNode run(ArrayNode input) {
		DescribeVoicesRequest describeVoicesRequest = new DescribeVoicesRequest()
				.withEngine(getData().getProperties().get("Engine").asText("standard"))
				.withLanguageCode(getData().getProperties().get("LanguageCode").asText("en-IN"));

		// Synchronously ask Amazon Polly to describe available TTS voices.
		DescribeVoicesResult describeVoicesResult = polly.describeVoices(describeVoicesRequest);
		for (Voice v : describeVoicesResult.getVoices()) {
			if (v.getName().equals(getData().getProperties().get("Name").asText("Aditi"))) {
				voice = v;
			}
		}
		return super.run(input);
	}


	@Override
	public ArrayNode runOne(ObjectNode input) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(input);
		if (voice != null) {
			StartSpeechSynthesisTaskResult result = 
				polly.startSpeechSynthesisTask(
					new StartSpeechSynthesisTaskRequest()
						.withEngine(getData().getProperties().get("Engine").asText("standard"))
						.withLanguageCode(getData().getProperties().get("LanguageCode").asText("en-IN"))
						.withVoiceId(voice.getId())
						.withOutputFormat("mp3")
						.withOutputS3BucketName(defaultBucket)
						.withOutputS3KeyPrefix(
								defaultPath
								+ 
								getData().getCorpusId() 
								+ "/" 
								+ getData().getCorpusBatchId()
								+ "/"
								+ getData().getCorpusBatchStepSequenceId() + "-" + getData().getCorpusBatchStepId()
								+ "/"
								+ getDocID(outputStepItem)
						)
						.withText(getTextDocumentFromStorage(input))
			);
			outputStepItem.put(
					"pollyStorage",
					result.getSynthesisTask().getOutputUri()
			);
			retval.add(outputStepItem);
		}
		return retval;
	}

}
