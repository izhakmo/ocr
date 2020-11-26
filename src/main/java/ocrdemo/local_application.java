package ocrdemo;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;



import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Random;

public class local_application {

    private static AmazonS3 s3;

    public static void main(String[] args) throws IOException {
        s3 = AmazonS3ClientBuilder.defaultClient();

        Bucket bucket =  s3.createBucket("moshehagever");


//        // Upload a text string as a new object.
//        s3.putObject("moshehagever", "stringObjKeyName", "Uploaded String Object");


        // Upload a file as a new object with ContentType and title specified.
        String objectKey = "fileObjKeyName";
        String path = "C:\\Users\\izhak\\IdeaProjects\\text.images.txt";     //TODO need to be args[0]

        PutObjectRequest request = new PutObjectRequest(bucket.getName(),objectKey , new File(path));
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("moshe Type");
        metadata.addUserMetadata("title", "Amir.Tal rock hard");
        request.setMetadata(metadata);
        s3.putObject(request);

//    } catch (AmazonServiceException e) {
//        // The call was transmitted successfully, but Amazon S3 couldn't process
//        // it, so it returned an error response.
//        e.printStackTrace();
//    } catch (SdkClientException e) {
//        // Amazon S3 couldn't be contacted for a response, or the client
//        // couldn't parse the response from Amazon S3.
//        e.printStackTrace();
//    }

        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

        String queue_url = sqs.createQueue("MyQueue" + new Date().getTime())
                .getQueueUrl();

        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queue_url)
                .withMessageBody(objectKey);

        sqs.sendMessage(send_msg_request);

//            TODO check Qs - open manager if we no manager is up ==> tags
//            TODO response
//            TODO terminate works etc .....
    }

}
