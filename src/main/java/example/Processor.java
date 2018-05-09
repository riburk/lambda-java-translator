package example;


import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
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
    private Regions REGION = Regions.US_WEST_2;
    private ObjectMapper objectMapper;
    private String STREAM_NAME = "translation-stream";

    public Void handleRequest(KinesisEvent event, Context context) {
        String data;
        VocabularyEvent vocabularyEvent;
        objectMapper = new ObjectMapper();
        this.initDynamoDbClient();

        for(KinesisEventRecord rec : event.getRecords()) {
            data = new String(rec.getKinesis().getData().array());
            System.out.println(data);
            try {
                vocabularyEvent = objectMapper.readValue(data, VocabularyEvent.class);
            } catch (IOException e) {
                System.out.println("there was an IOException in the Object Mapper");
                return null;
            }
            if (vocabularyEvent.getEventType() == "AddWord") {
                persistWord(vocabularyEvent.getWord());
            }
            if (vocabularyEvent.getEventType() == "TranslateWord") {
                translateWord(vocabularyEvent.getWord());
            }
        }
        return null;
    }

    private void persistWord(String word)
            throws ConditionalCheckFailedException {

        HashMap<String,AttributeValue> item_values =
                new HashMap<String,AttributeValue>();

        item_values.put("Word", new AttributeValue(word));

        this.amazonDynamoDB.putItem(DYNAMODB_TABLE_NAME, item_values);

        SendTranslationEvent(word);
    }

    private void translateWord(String word) {

        String translatedWord = translateText(word);

        DynamoDB dynamoDB = new DynamoDB(this.amazonDynamoDB);
        Table table = dynamoDB.getTable(DYNAMODB_TABLE_NAME);

        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("Word", word)
                .withUpdateExpression("set French = :t")
                .withValueMap(new ValueMap().withString(":t", translatedWord))
                .withReturnValues(ReturnValue.UPDATED_NEW);

        try {
            System.out.println("Updating the item...");
            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
            System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());

        }
        catch (Exception e) {
            System.err.println("Unable to update item: " + word);
            System.err.println(e.getMessage());
        }
    }

    private void initDynamoDbClient() {
        amazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
    }

    private static String translateText(String sourceText) {
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

    private void SendTranslationEvent(String word) {

        KinesisProducer kinesis = new KinesisProducer();
        objectMapper = new ObjectMapper();
        ByteBuffer data = null;

        VocabularyEvent vocabularyEvent = new VocabularyEvent("TranslateWord", word);

        try {
            data = ByteBuffer.wrap(objectMapper.writeValueAsBytes(vocabularyEvent));
        } catch (JsonProcessingException e) {
            System.out.println("There was an error converting the event to json");
        }

        kinesis.addUserRecord(STREAM_NAME, "myPartitionKey", data);
    }
}