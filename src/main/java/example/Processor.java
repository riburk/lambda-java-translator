package example;


import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;

public class Processor implements RequestHandler<KinesisEvent, Void> {

    private AmazonDynamoDB amazonDynamoDB;
    private String DYNAMODB_TABLE_NAME = "Vocabulary";
    private Regions REGION = Regions.US_WEST_2;
    private ObjectMapper objectMapper;


//    public Void handleRequest(KinesisEvent event, Context context)
//    {
//        for(KinesisEventRecord rec : event.getRecords()) {
//            System.out.println(new String(rec.getKinesis().getData().array()));
//            LambdaLogger logger = context.getLogger();
//            logger.log(new String(rec.getKinesis().getData().array()));
//        }
//        return null;
//    }

    public Void handleRequest(KinesisEvent event, Context context) {
        String data;
        AddWordRequest addWordRequest;
        objectMapper = new ObjectMapper();
        this.initDynamoDbClient();
        final AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withRegion(REGION)
                .build();
        for(KinesisEventRecord rec : event.getRecords()) {
            data = new String(rec.getKinesis().getData().array());
            System.out.println(data);
//            AddWordRequest result = objectMapper.treeToValue(rec.getKinesis().getData(), AddWordRequest.class);
            try {
                addWordRequest = objectMapper.readValue(data, AddWordRequest.class);
            } catch (IOException e) {
                System.out.println("there was an IOException in the Object Mapper");
                return null;
            }
            System.out.println(addWordRequest.getWord());
            persistData(amazonDynamoDB, addWordRequest.getWord());
        }
        return null;
    }

    private PutItemResult persistData(AmazonDynamoDB amazonDynamoDB, String word)
            throws ConditionalCheckFailedException {
//        return this.dynamoDb.getTable(DYNAMODB_TABLE_NAME)
//                .putItem(
//                        new PutItemSpec().withItem(new Item()
//                                .withString("Word", word)));
        HashMap<String,AttributeValue> item_values =
                new HashMap<String,AttributeValue>();

        item_values.put("Word", new AttributeValue(word));

//        for (String[] field : extra_fields) {
//            item_values.put(field[0], new AttributeValue(field[1]));
//        }

        return this.amazonDynamoDB.putItem(DYNAMODB_TABLE_NAME, item_values);
    }

    private void initDynamoDbClient() {
        amazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
    }
}