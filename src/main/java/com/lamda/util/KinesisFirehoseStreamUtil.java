package com.lamda.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.s3.model.Region;

public class KinesisFirehoseStreamUtil extends AwsCredentialsConfig {
	private AmazonKinesisFirehoseClient firehoseClient = null;
	private final String firehoseStreamName = "test";

	public AmazonKinesisFirehoseClient getKinesisFirehoseClient() {
		firehoseClient = new AmazonKinesisFirehoseClient(credentials);
		firehoseClient.setRegion(Region.US_Standard.toAWSRegion());
		return firehoseClient;
	}

	public void writeToStream(String jsonData) {

		ByteBuffer data;
		try {
			data = ByteBuffer.wrap(jsonData.getBytes("UTF-8"));
			PutRecordRequest putRecordRequest = new PutRecordRequest();
			putRecordRequest.setDeliveryStreamName(firehoseStreamName);
			Record record = new Record().withData(data);
			putRecordRequest.setRecord(record);
			// Put record into the DeliveryStream
			firehoseClient.putRecord(putRecordRequest);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			System.out.println("Error getting bytes KinesisFirehoseStreamUtil.writeToStream function" + e.getMessage());
			e.printStackTrace();
		}
	}
	
}
