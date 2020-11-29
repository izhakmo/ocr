package ocrdemo;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;



import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class local_application {

    private static AmazonS3 s3;

    public static void main(String[] args) throws IOException {
        s3 = AmazonS3ClientBuilder.defaultClient();

//        key pair request

        String key_pair_string = "key"+new Date().getTime();
        CreateKeyPairRequest createKeyPairRequest = new CreateKeyPairRequest();
        createKeyPairRequest.withKeyName(key_pair_string);

//        TODO check tags to see if manager is up
//        TODO get manager or create one

        AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();

//      continue with key after we got the manager

        CreateKeyPairResult createKeyPairResult = ec2.createKeyPair(createKeyPairRequest);

        KeyPair keyPair = new KeyPair();
        keyPair = createKeyPairResult.getKeyPair();

        String privateKey = keyPair.getKeyMaterial();

        Tag tag = new Tag();
        tag.setKey("manager");
        tag.setValue("manager");



        TagSpecification tag_specification = new TagSpecification();

        //TODO check if a manager instance is active

        RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
        runInstancesRequest.withImageId("ami-04d29b6f966df1537")
                .withInstanceType(InstanceType.T2Micro)
                .withMinCount(1).withMaxCount(1)
                .withKeyName(key_pair_string)  //TODO ?????
                .withSecurityGroupIds("sg-4f791b7d")
                .withTagSpecifications(tag_specification);



        RunInstancesResult result = ec2.runInstances(runInstancesRequest);

        String bucket_name = "moshehagever";
        Bucket bucket =  s3.createBucket(bucket_name);




        // Upload a file as a new object with ContentType and title specified.
        String file_to_upload = "fileObjKeyName" + new Date().getTime();
        String path = "C:\\Users\\izhak\\IdeaProjects\\text.images.txt";     //TODO need to be args[0]

        PutObjectRequest request = new PutObjectRequest(bucket.getName(),file_to_upload , new File(path));
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

        String local_to_manager = sqs.createQueue("local_to_manager" + new Date().getTime())
                .getQueueUrl();

        String manager_to_local = sqs.createQueue("manager_to_local" + new Date().getTime())
                .getQueueUrl();

        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(local_to_manager)
                .withMessageBody(file_to_upload);

        sqs.sendMessage(send_msg_request);

        List<Message> messages = sqs.receiveMessage(manager_to_local).getMessages();

//        GetObjectRequest object_request = new GetObjectRequest(bucket_name,messages.get(0).getBody());
//
//        S3Object o = s3.getObject(object_request);
//        S3ObjectInputStream object_content = o.getObjectContent();


//        Map<String,String> attributes =  messages.get(0).getAttributes();
//        if (attributes.containsKey("done task")){



        if(args.length > 4 && args[4].equals("terminate")) {
            SendMessageRequest terminate_request = new SendMessageRequest()
                    .withQueueUrl(local_to_manager)
                    .withMessageBody("terminate");

            sqs.sendMessage(terminate_request);
        }
//            //TODO terminate manager at EC2
//        }

//            TODO check Qs - open manager if we no manager is up ==> tags
//            TODO response
//            TODO terminate works etc .....
    }

}
