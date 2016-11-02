package com.lamda.event;

import java.io.IOException;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

public class KinesisToDynamoDbWriter {
	public void recordHandler(KinesisEvent event) throws IOException {
		DynamoDbClientV2 client = new DynamoDbClientV2();
		for (KinesisEventRecord rec : event.getRecords()) {
			System.out.println(new String(rec.getKinesis().getData().array()));
			client.putItemWithJson();
			;
		}
		client.batchPutItems();
		System.out.println("batch put items done!!");
	}
}

/*
 * 
 curl http://localhost:8983/solr/demo/update -d ' [ {"id" : "123442234242", "name"
  : "Rhutwij", "tt" : "Brandon Sanderson","manage" : "Brandon Sanderson", "roles":["engineer","manager"]
  } ]'
 */