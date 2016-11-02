package com.lamda.event;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
import com.amazonaws.services.s3.model.Region;

public class WriteToFirehoseDeliveryStream {
	public void firehoseRecordHandler(KinesisEvent event) throws IOException {
		try {
			AWSCredentials credentials=new PropertiesCredentials(
					WriteToFirehoseDeliveryStream.class
					.getResourceAsStream("/awscredentials.properties"));
			AmazonKinesisFirehoseClient firehoseClient = new AmazonKinesisFirehoseClient(credentials);
			firehoseClient.setRegion(Region.US_Standard.toAWSRegion());
			for (KinesisEventRecord rec : event.getRecords()) {
				System.out.println(new String(rec.getKinesis().getData().array()));
				ByteBuffer data = ByteBuffer.wrap(new String(rec.getKinesis().getData().array()).getBytes("UTF-8"));
				PutRecordRequest putRecordRequest = new PutRecordRequest();
                putRecordRequest.setDeliveryStreamName("test");
                Record record = new Record().withData(data);
                putRecordRequest.setRecord(record);

                // Put record into the DeliveryStream
                firehoseClient.putRecord(putRecordRequest);
				
			}
			System.out.println("putting record to kinesisFirehoseStream");
		} catch (Exception E) {

		}
	}
}
