package com.lamda.event;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;

public class DynamoDbClientV2 {
	private final String tableName = "IdentityIQ-test-rhutwij";
	private static AmazonDynamoDBClient client=null;
	private List<Item> itemList;
	
	public static AmazonDynamoDBClient getAmazonDynamoDBClient() throws IOException
	{
		AWSCredentials credentials=new PropertiesCredentials(
				DynamoDBClient.class
				.getResourceAsStream("/awscredentials.properties"));
		client = new AmazonDynamoDBClient(credentials);
		
		return client;
		
	}
	
	public static int randInt(int min, int max) {

	    // Usually this can be a field rather than a method variable
	    Random rand = new Random();

	    // nextInt is normally exclusive of the top value,
	    // so add 1 to make it inclusive
	    int randomNum = rand.nextInt((max - min) + 1) + min;

	    return randomNum;
	}
	
	public void batchPutItems()
	{
		 

		 String json1= "{"
					+   "\"identity-id\" : 123114 ,"
			        +   "\"application\" : \"IdentityNow\" ,"
			        +   "\"instance\" : \"random\" ,"
			        +   "\"DisplayName\" : \"JianXu\" ,"
			        +   "\"current_city\" : \"Austin\" ,"
			        +   "\"date\" : {"
			        +       "\"role\" : 2013 ,"
			        +       "\"month\" : 11 ,"
			        +       "\"day\" : 12"
			        +   "} ,"
			        +   "\"roles\" :"
			        +   "[ \"SJB\" , \"DD\" , \"MM\" , \"FF\" , \"GTB\" ]"
			        + "}"
			        ;
		 
		 String json2= "{"
					+   "\"identity-id\" : 565673434 ,"
			        +   "\"application\" : \"IdentityNow\" ,"
			        +   "\"instance\" : \"random\" ,"
			        +   "\"DisplayName\" : \"Rohan\" ,"
			        +   "\"current_city\" : \"San Jose\" ,"
			        +   "\"date\" : {"
			        +       "\"role\" : 2014 ,"
			        +       "\"month\" : 10 ,"
			        +       "\"day\" : 22"
			        +   "} ,"
			        +   "\"roles\" :"
			        +   "[ \"MM\" , \"FF\" , \"MM\" , \"GF\" , \"GTB\" ]"
			        + "}"
			        ;
		 
		 TableWriteItems identityTableWrites = new TableWriteItems(tableName);
		 List<Item> itemsToPut= new ArrayList<Item>();
		 
		 itemsToPut.add(new Item()
			      .withPrimaryKey("identity-id", "123114")
			      .withJSON("document", json1));
		 itemsToPut.add(new Item()
			      .withPrimaryKey("identity-id", "565673434")
			      .withJSON("document", json2));
		
		 identityTableWrites.withItemsToPut(itemsToPut);
		 
		 AmazonDynamoDBClient client;
		try {
			client = DynamoDbClientV2.getAmazonDynamoDBClient();
			 DynamoDB dynamoDB = new DynamoDB(client);
			 dynamoDB.batchWriteItem(identityTableWrites);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
	}
	
	public List<Item> matchEventsToItem(KinesisEvent event)
	{
	
		itemList = null;
		String tempId=null;
		for (KinesisEventRecord rec : event.getRecords())
		{
			String json=rec.getKinesis().getData().toString();
			JsonObject streamData=Json.parse(json).asObject();;
		    streamData.getString("identity-id", tempId);
		    itemList.add(new Item()
					      .withPrimaryKey("identity-id", tempId)
					      .withJSON("document", json) );
		    
		    
			//rec.getKinesis().getData().array();
		}
		return itemList;
		
	}
	
	
	public void putItemWithJson()
	{
		
		try {
			AmazonDynamoDBClient client = DynamoDbClientV2.getAmazonDynamoDBClient();
			
			/*
			 * class	String, always "link"
				application	String
				instance	String
				nativeIdentity	String
				displayNae
			 */
			
			List<String> cities=new ArrayList<String>();
			cities.add("Austin");
			cities.add("Boston");
			cities.add("San Jose");
			cities.add("New York");
			int rand=DynamoDbClientV2.randInt(0, 3);
			String city=cities.get(rand);
			
			List<String> roles=new ArrayList<String>();
			roles.add("DD");
			roles.add("ASB");
			roles.add("TT");
			roles.add("JJ");
			
			String role=roles.get(rand);
			
			
			List<String> names=new ArrayList<String>();
			names.add("Danielle");
			names.add("Jian");
			names.add("Neha");
			names.add("Carl");
			
			String name=names.get(rand);
			
			
			
			
			String json = "{"
					+   "\"identity-id\" : 123114 ,"
			        +   "\"application\" : \"IdentityNow\" ,"
			        +   "\"instance\" : \"random\" ,"
			        +   "\"DisplayName\" : \""+name+"\" ,"
			        +   "\"current_city\" : \""+city+"\" ,"
			        +   "\"date\" : {"
			        +       "\"role\" : 2015 ,"
			        +       "\"month\" : 11 ,"
			        +       "\"day\" : 12"
			        +   "} ,"
			        +   "\"roles\" :"
			        +   "[ \"SJB\" , \""+role+"\" , \"MM\" , \""+role+"\" , \"GTB\" ]"
			        + "}"
			        ;
			
			DynamoDB dynamoDB = new DynamoDB(client);
			Table table = dynamoDB.getTable(tableName);
			
			Item item =
					  new Item()
					      .withPrimaryKey("identity-id", name+Math.random())
					      .withJSON("document", json);

			table.putItem(item);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	

}
