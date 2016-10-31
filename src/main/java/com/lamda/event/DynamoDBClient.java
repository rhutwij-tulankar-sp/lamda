package com.lamda.event;

import java.io.IOException;
import java.text.SimpleDateFormat;

//dynamo imports

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
public class DynamoDBClient {
	private final String tableName = "TempTable";
	private AmazonDynamoDBClient client = null;

	public DynamoDBClient() throws IOException {
		AWSCredentials credentials = new PropertiesCredentials(
				DynamoDBClient.class
						.getResourceAsStream("/awscredentials.properties"));
		client = new AmazonDynamoDBClient(credentials);
	}
	
/*	public static void main(String[] args)
	{
		System.out.println(DynamoDBClient.class
						.getResourceAsStream("/awscredentials.properties"));
	}
*/
	
	public static void logMessage(String msg) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println(sdf.format(new Date()) + " ==> " + msg);
	}

	
	public void putItems() {
		logMessage("Putting items into table " + tableName);
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        
		
		// Add bikes.
		item.put("id", new AttributeValue().withN("123"));
		// Size, followed by some title.
		item.put("name", new AttributeValue().withS("18-Bike-201"));
		PutItemRequest itemRequest = new PutItemRequest().withTableName(
				tableName).withItem(item);
		client.putItem(itemRequest);
		item.clear();
	}


	/*public void listItems() {
		logMessage("List all items");
		ScanRequest scanRequest = new ScanRequest().withTableName(tableName);

		ScanResult result = client.scan(scanRequest);
		for (Map<String, AttributeValue> item : result.getItems()) {
			printItem(item);
		}
	}*/


}