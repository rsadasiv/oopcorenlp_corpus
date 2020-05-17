package io.outofprintmagazine.corpus.storage.s3;

import java.io.IOException;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class S3Utils {

	private S3Utils() {
		super();
	}
	
	private static S3Utils single_instance = null; 

    public static S3Utils getInstance() throws IOException { 
        if (single_instance == null) 
            single_instance = new S3Utils(); 
  
        return single_instance; 
    }
	
	public AmazonS3 getS3Client() throws IOException {
		return AmazonS3ClientBuilder.standard()
			      .withCredentials(new AWSStaticCredentialsProvider(AwsUtils.getInstance().getBasicCredentials()))
			      .withRegion(AwsUtils.getInstance().getRegion())
			      .build();
	}

}
