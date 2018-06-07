package TranslationHandler;


import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.cloud.translate.Translate;
import com.google.cloud.translate.Translate.TranslateOption;
import com.google.cloud.translate.TranslateOptions;
import com.google.cloud.translate.Translation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

public class Processor implements RequestHandler<KinesisEvent, Void> {

    private AmazonDynamoDB amazonDynamoDB;
    private String DYNAMODB_TABLE_NAME = "Vocabulary";
    private String REGION = "us-west-2";
    private ObjectMapper objectMapper;
    private String STREAM_NAME = "translation-stream";

    public Void handleRequest(KinesisEvent event, Context context) {
        String data;
        VocabularyEvent vocabularyEvent;
        objectMapper = new ObjectMapper();
        this.initDynamoDbClient();

        for (KinesisEventRecord rec : event.getRecords()) {
            data = new String(rec.getKinesis().getData().array());
            System.out.println(data);
            try {
                vocabularyEvent = objectMapper.readValue(data, VocabularyEvent.class);
            } catch (IOException e) {
                System.out.println("there was an IOException in the Object Mapper");
                return null;
            }
            System.out.printf("EventType: %s\n", vocabularyEvent.getEventType());
            if (vocabularyEvent.getEventType().equals("AddWord")) {
                System.out.println("persisting word");
                persistWord(vocabularyEvent);
            }
            if (vocabularyEvent.getEventType().equals("TranslateWord")) {
                System.out.println("translating word");
                translateWord(vocabularyEvent);
            }
        }
        return null;
    }

    private void persistWord(VocabularyEvent event) {
        System.out.println("Entering persistWord");
        String word = event.getWord();

        HashMap<String, AttributeValue> item_values =
                new HashMap<String, AttributeValue>();

        item_values.put("Word", new AttributeValue(word));

        try {
            this.amazonDynamoDB.putItem(DYNAMODB_TABLE_NAME, item_values);


            sendTranslationEvent(word);
        } catch (Exception e) {
            System.err.println("Unable to put item: " + word);
            System.err.println(e.getMessage());
            storeForRetry(event);
        }
        System.out.println("Exiting persistWord");

    }

    private void translateWord(VocabularyEvent event) {
        System.out.println("Entering translateWord");
        String word = event.getWord();

        try {
            String translatedWord = translateText(word);

            DynamoDB dynamoDB = new DynamoDB(this.amazonDynamoDB);
            Table table = dynamoDB.getTable(DYNAMODB_TABLE_NAME);

            UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("Word", word)
                    .withUpdateExpression("set French = :t")
                    .withValueMap(new ValueMap().withString(":t", translatedWord))
                    .withReturnValues(ReturnValue.UPDATED_NEW);


            System.out.println("Updating the item...");
            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
            System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());

        } catch (Exception e) {
            System.err.println("Unable to update item: " + word);
            System.err.println(e.getMessage());
            storeForRetry(event);
        }
        System.out.println("Exiting translateWord");
    }

    private void initDynamoDbClient() {
        amazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
    }

    private String translateText(String sourceText) throws Exception {
//        System.out.println("CAUSING A BIG FAILURE!!!!");
        causeFakeFailure("translation");

        Translate translate = createTranslateService();
        TranslateOption srcLang = TranslateOption.sourceLanguage("en");
        TranslateOption tgtLang = TranslateOption.targetLanguage("fr");

        Translation translation = translate.translate(sourceText, srcLang, tgtLang);

        System.out.printf("Source Text:\n\t%s\n", sourceText);
        System.out.printf("Translated Text:\n\t%s\n", translation.getTranslatedText());
        return translation.getTranslatedText();
    }

    private static Translate createTranslateService() {
        return TranslateOptions.newBuilder().build().getService();
    }


    private void sendTranslationEvent(String word) {
        try {
            VocabularyEvent vocabularyEvent = new VocabularyEvent("TranslateWord", word);
            objectMapper = new ObjectMapper();
            ByteBuffer data = ByteBuffer.wrap(objectMapper.writeValueAsBytes(vocabularyEvent));

            AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();

            clientBuilder.setRegion(REGION);
            //        clientBuilder.setCredentials(credentialsProvider);
            //        clientBuilder.setClientConfiguration(config);

            AmazonKinesis kinesisClient = clientBuilder.build();

            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(STREAM_NAME);
            putRecordRequest.setData(data);
            putRecordRequest.setPartitionKey("myPartitionKey");
            //        putRecordRequest.setSequenceNumberForOrdering( sequenceNumberOfPreviousRecord );
            PutRecordResult putRecordResult = kinesisClient.putRecord(putRecordRequest);
            //        sequenceNumberOfPreviousRecord = putRecordResult.getSequenceNumber();
            System.out.println("Result Shard Id: " + putRecordResult.getShardId());
        } catch (JsonProcessingException e) {
            System.out.println("There was a json processing exception");
        }

    }

    private void storeForRetry(VocabularyEvent event) {
        // Save to S3
    }

    private void causeFakeFailure(String location) throws Exception {
        DynamoDB dynamoDB = new DynamoDB(this.amazonDynamoDB);
        Table table = dynamoDB.getTable("VocabularyFailure");

        Item item = table.getItem("Location", location);

        System.out.printf("failure = %s\n", item.getBOOL("failure"));

        if(item.getBOOL("failure")) {
            throw new Exception("Fake failure initiated in " + location);
        }
    }
}