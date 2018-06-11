package Translation;

import java.io.*;
import java.util.Date;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.lambda.runtime.Context;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class RetryHandler implements RequestStreamHandler {
    private String REGION = "us-west-2";
    private ObjectMapper objectMapper;
    private String S3_BUCKET_NAME = "translate-lambda";


    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context)  {

        String clientRegion = REGION;
        String bucketName = S3_BUCKET_NAME;
        String retryDirName = "retry-files";
        String retryFileName = new Date().toString();

        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .build();

            // Upload a text string as a new object.
            ObjectListing objectListing = s3Client.listObjects(bucketName, retryDirName);
            for (S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) {
                //System.out.println(s3ObjectSummary.getKey());
//                System.out.println(s3Client.getObject(S3_BUCKET_NAME, s3ObjectSummary.getKey());
                S3Object fullObject = s3Client.getObject(S3_BUCKET_NAME, s3ObjectSummary.getKey());
                displayTextInputStream(fullObject.getObjectContent());
            }
        }
        catch(AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        }
        catch(SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
        catch(java.io.IOException e) {
            e.printStackTrace();
        }
    }

    private static void displayTextInputStream(InputStream input) throws IOException {
        // Read the text input stream one line at a time and display each line.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        System.out.println();
    }

//    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
//
//        LambdaLogger logger = context.getLogger();
//        logger.log("Loading Java Lambda handler of ProxyWithStream");
//
//
//        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
//        JSONObject responseJson = new JSONObject();
//        String name = "you";
//        String city = "World";
//        String time = "day";
//        String day = null;
//        String responseCode = "200";
//
//        try {
//            JSONObject event = (JSONObject)parser.parse(reader);
//            if (event.get("queryStringParameters") != null) {
//                JSONObject qps = (JSONObject)event.get("queryStringParameters");
//                if ( qps.get("name") != null) {
//                    name = (String)qps.get("name");
//                }
//            }
//
//            if (event.get("pathParameters") != null) {
//                JSONObject pps = (JSONObject)event.get("pathParameters");
//                if ( pps.get("proxy") != null) {
//                    city = (String)pps.get("proxy");
//                }
//            }
//
//            if (event.get("headers") != null) {
//                JSONObject hps = (JSONObject)event.get("headers");
//                if ( hps.get("day") != null) {
//                    day = (String)hps.get("day");
//                }
//            }
//
//            if (event.get("body") != null) {
//                JSONObject body = (JSONObject)parser.parse((String)event.get("body"));
//                if ( body.get("time") != null) {
//                    time = (String)body.get("time");
//                }
//            }
//
//            String greeting = "Good " + time + ", " + name + " of " + city + ". ";
//            if (day!=null && day != "") greeting += "Happy " + day + "!";
//
//
//            JSONObject responseBody = new JSONObject();
//            responseBody.put("input", event.toJSONString());
//            responseBody.put("message", greeting);
//
//            JSONObject headerJson = new JSONObject();
//            headerJson.put("x-custom-header", "my custom header value");
//
//            responseJson.put("isBase64Encoded", false);
//            responseJson.put("statusCode", responseCode);
//            responseJson.put("headers", headerJson);
//            responseJson.put("body", responseBody.toString());
//
//        } catch(ParseException pex) {
//            responseJson.put("statusCode", "400");
//            responseJson.put("exception", pex);
//        }
//
//        logger.log(responseJson.toJSONString());
//        OutputStreamWriter writer = new OutputStreamWriter(outputStream, "UTF-8");
//        writer.write(responseJson.toJSONString());
//        writer.close();
//    }
}