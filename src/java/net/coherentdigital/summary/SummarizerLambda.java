package net.coherentdigital.summary;

import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.core.ResponseInputStream;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


public class SummarizerLambda implements RequestHandler<SQSEvent, Void>{

	public final static org.slf4j.Logger LOG = LoggerFactory.getLogger(TextSummarizer.class);

    @Override
    public Void handleRequest(SQSEvent event, Context context)
    {
	    for(SQSMessage msg : event.getRecords()){
	    	try {
		        String body = new String(msg.getBody());
			    JSONParser parser = new JSONParser();
			    JSONObject jo = (JSONObject)parser.parse(body);

		    	String bucket = (String)jo.get("bucket");
		    	String key = (String)jo.get("key");
		    	String outKey = (String)jo.get("outKey");
		    	int count = ((Long)jo.get("count")).intValue();

		    	LOG.info(String.format("Extracting summary from s3://%s/%s", bucket, key));

		    	Region region = Region.US_EAST_2;
		    	S3Client s3 = S3Client.builder().region(region).httpClientBuilder(ApacheHttpClient.builder()).build();
		    	GetObjectRequest getObjectRequest = GetObjectRequest.builder()
			    	.bucket(bucket)
			    	.key(key)
			    	.build();

		    	ResponseInputStream<GetObjectResponse> responseInputStream = s3.getObject(getObjectRequest);
		    	String content = new String(responseInputStream.readAllBytes(), StandardCharsets.UTF_8);
		    	String summary = new TextSummarizer().summarize(content, count);

		    	PutObjectRequest objectRequest = PutObjectRequest.builder()
			    	.bucket(bucket)
			    	.key(outKey)
			    	.build();

		    	s3.putObject(objectRequest, RequestBody.fromString(summary));

		    	LOG.info(String.format("Summary has been successfully extracted into s3://%s/%s", bucket, outKey));
	    	} catch (Exception e) {
	        	LOG.error("Extraction failed.", e);
	      	}
	    }

    	return null;
    }
}