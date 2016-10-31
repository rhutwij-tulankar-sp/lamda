package com.lamda.event;

import java.io.IOException;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;



public class DynamoDb {
	public void recordHandler(KinesisEvent event) throws IOException {
		DynamoDBClient client= new DynamoDBClient();
		for (KinesisEventRecord rec : event.getRecords()) {
			System.out.println(new String(rec.getKinesis().getData().array()));
			client.putItems();
		}
	}
}
