package com.lamda.event;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
import java.util.List;
import java.util.ArrayList;

public class WriteToKinesisStream {

	public void kinesisRecordHandler(KinesisEvent event) throws IOException, InterruptedException {
		try {
			AWSCredentials credentials=new PropertiesCredentials(
					DynamoDBClient.class
					.getResourceAsStream("/awscredentials.properties"));
			//AmazonKinesisFirehoseClient firehoseClient = new AmazonKinesisFirehoseClient(credentials);
			AmazonKinesisClient amazonKinesisClient = new AmazonKinesisClient(credentials);
			PutRecordsRequest putRecordsRequest  = new PutRecordsRequest();
			putRecordsRequest.setStreamName("test");
			List <PutRecordsRequestEntry> putRecordsRequestEntryList  = new ArrayList<PutRecordsRequestEntry>();
			for (KinesisEventRecord rec : event.getRecords()) {
				System.out.println(new String(rec.getKinesis().getData().array()));

				ByteBuffer data = ByteBuffer.wrap(new String(rec.getKinesis().getData().array()).getBytes("UTF-8"));
				PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
				putRecordsRequestEntry.setData(data);
				putRecordsRequestEntry.setPartitionKey("123");
				putRecordsRequestEntryList.add(putRecordsRequestEntry); 

			}
			putRecordsRequest.setRecords(putRecordsRequestEntryList);
			PutRecordsResult putRecordsResult  = amazonKinesisClient.putRecords(putRecordsRequest);
			System.out.println("Put Result" + putRecordsResult);

		} catch (Exception E) {

			System.out.println(E.getMessage()+E.getStackTrace());
		}

		System.out.println("--------Finished writing each record to kinesis firehose stream-------");
	}

}
