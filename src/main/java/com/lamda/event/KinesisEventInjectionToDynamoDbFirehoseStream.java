package com.lamda.event;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
import com.lamda.util.DynamoDbUtil;
import com.lamda.util.KinesisFirehoseStreamUtil;

public class KinesisEventInjectionToDynamoDbFirehoseStream {

	public void recordDataPipelineHandler(KinesisEvent event) throws IOException {

		// get dynamodb client
		DynamoDbUtil dynamoUtil = new DynamoDbUtil();

		// get firehose client
		KinesisFirehoseStreamUtil firehoseUtil = new KinesisFirehoseStreamUtil();

		// List of items to batch write to DynamoDb
		List<Item> itemsToPutDynamoDb = new ArrayList<Item>();

		String jsonEvent = null;
		for (KinesisEventRecord rec : event.getRecords()) {
			try {
				jsonEvent = rec.getKinesis().getData().toString();
				// write to list of items for KinesisFirehose
				firehoseUtil.writeToStream(jsonEvent);
				
				// map events to items / Json validation check and add Item to itemsToPutDynamoDb
				Item jsonEventItem=dynamoUtil.jsonToItemConverter(jsonEvent);
				
				//if primary key present then add to list else dont
				if(jsonEventItem!=null)
				itemsToPutDynamoDb.add(jsonEventItem);
				
				//debug 
				System.out.println(new String(rec.getKinesis().getData().array()));
				
			} catch (Exception e) {
                System.out.println("Exception in KinesisEventInjectionToDynamoDbFirehoseStream.recordDataPipelineHandler"+e.getMessage());
                //send sns alarm or counter measure
			}
		}

		System.out.println("----Batch putting items to dynamoDb----");
		dynamoUtil.batchPutItems(itemsToPutDynamoDb);
		System.out.println("----batch put items done!!----");
	}
}
