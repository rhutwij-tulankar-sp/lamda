package com.lamda.util;

import java.io.IOException;
import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;

public class DynamoDbUtil extends AwsCredentialsConfig {

	private final String tableName = "IdentityIQ-test-rhutwij";
	private DynamoDB dynamoDB = null;

	public DynamoDB getAmazonDynamoDBClient() throws IOException {
		dynamoDB = new DynamoDB(new AmazonDynamoDBClient(credentials));
		return dynamoDB;

	}

	public void batchPutItems(List<Item> kinesisEventJson) {

		TableWriteItems identityTableWrites = new TableWriteItems(tableName);
		identityTableWrites.withItemsToPut(kinesisEventJson);

		DynamoDB dynamoDBClient;
		try {
			dynamoDBClient = getAmazonDynamoDBClient();
			dynamoDBClient.batchWriteItem(identityTableWrites);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error batch writing DynamoDbUtil.batchPutItems" + e.getMessage());
			e.printStackTrace();
		}

	}
	
	@SuppressWarnings("unused")
	public Item jsonToItemConverter(String  json)
	{
		JsonObject streamData=Json.parse(json).asObject();
		String tempId=null;
		streamData.getString("identity-id", tempId);
		Item jsonItem=null;
		if(tempId!=null)
		{
			jsonItem =new Item().withPrimaryKey("identity-id", tempId).withJSON("document", json);
		}
		else
		{
			//send sns alarm
		}
	    return jsonItem;
	}

}
