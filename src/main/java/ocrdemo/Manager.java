package ocrdemo;


import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import java.io.*;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class Manager {

    private static void displayTextInputStream(InputStream input) throws IOException {
        // Read the text input stream one line at a time and display each line.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = null;



        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        System.out.println();
    }

    public static void main(String[] args) throws IOException {
        AmazonS3 s3 = local_application.s3;
        AmazonEC2 ec2 = local_application.ec2;
        AmazonSQS sqs = local_application.sqs;
        String managerID = local_application.GetManager(ec2);
        System.out.println("QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ\n managerID: "+ managerID);

        String local_to_managerSQS = sqs.getQueueUrl("local-to-manager-sqs" + managerID).getQueueUrl();

        String bucketName = "manager-bucket-"+managerID;

        List<Message> messages = sqs.receiveMessage(local_to_managerSQS).getMessages();
        int msg_to_manager_Counter = 1;
        int msg_to_workers_queue_counter = 0;


        while (! messages.isEmpty()) {
            while (!messages.isEmpty()) {
                Message msg = messages.remove(0);
//                Map<String, String> number = msg.getAttributes();
//                if(number.containsValue("3"))
//                    System.out.println("ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ");
//
//                System.out.println("ZZZZZZZZZZZZZZZZZZZZZZZ \n\n"+number+ "\n\n");

//                String msgBody = msg.getBody().substring(0,msg.getBody().indexOf("xxxxxx"));
                String msgBody = msg.getBody();
                String number = msg.getBody().substring(msg.getBody().indexOf("xxxxxx")+6);
                GetObjectRequest object_request = new GetObjectRequest(bucketName,msgBody);

                System.out.println("ZZZZZZZZZZZZZZZZZZZZZZZ \n\n"+number+ "\n\n");


                S3Object o = s3.getObject(object_request);
                S3ObjectInputStream object_content = o.getObjectContent();
                System.out.println(msg_to_manager_Counter+". \n\n"+" ====================================\n\n");
//                displayTextInputStream(object_content);


                BufferedReader reader = new BufferedReader(new InputStreamReader(object_content));
                String line = null;
                String manager_to_workers_queue;
                try {
                    manager_to_workers_queue = sqs.getQueueUrl("manager"+managerID + "_to_workers").getQueueUrl();
                }
                catch (QueueDoesNotExistException e){
                    manager_to_workers_queue = sqs.createQueue("manager"+managerID + "_to_workers").getQueueUrl();
                }

                while ((line = reader.readLine()) != null) {
                    if(line.length()==0)    {  continue; }

                    System.out.println(line);
                    SendMessageRequest url_msg_request_to_worker = new SendMessageRequest()
                            .withQueueUrl(manager_to_workers_queue)
                            .withMessageBody(line);

                    sqs.sendMessage(url_msg_request_to_worker);

                    msg_to_workers_queue_counter++;

                }
                int number_of_workers = msg_to_workers_queue_counter/ Integer.parseInt(number)+1;


//                TODO find solution
                String key_pair_string = "key"+new Date().getTime();
                CreateKeyPairRequest createKeyPairRequest = new CreateKeyPairRequest();
                createKeyPairRequest.withKeyName(key_pair_string);

                CreateKeyPairResult createKeyPairResult = ec2.createKeyPair(createKeyPairRequest);

                KeyPair keyPair = new KeyPair();
                keyPair = createKeyPairResult.getKeyPair();


                Tag tag = new Tag();
                tag.setKey("manager");
                tag.setValue("manager");
                CreateTagsRequest tagsRequest = new CreateTagsRequest().withTags(tag);

                tagsRequest.withTags(tag);

                TagSpecification tag_specification = new TagSpecification();


                RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
                runInstancesRequest.withImageId("ami-04d29b6f966df1537")
                        .withInstanceType(InstanceType.T2Micro)
                        .withMinCount(number_of_workers).withMaxCount(number_of_workers)
                        .withKeyName(key_pair_string)  //TODO ?????
                        .withSecurityGroupIds("sg-4f791b7d")
                        .withTagSpecifications(tag_specification);


                RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);
                managerID = runInstancesResult.getReservation().getInstances().get(0).getInstanceId();


                CreateTagsRequest tag_request = new CreateTagsRequest()
                        .withTags(tag)
                        .withResources(managerID);

                CreateTagsResult tag_response = ec2.createTags(tag_request);


                System.out.println("msg_to_workers_queue_counter: "+ msg_to_workers_queue_counter);


                System.out.println(msg_to_manager_Counter + ". msgBody: " + msgBody);
                msg_to_manager_Counter++;

//                File download_file_from_s3 =  new File(s3.getUrl(bucketName,msgBody).getFile());
                System.out.println(msg_to_manager_Counter + ". File: "+ s3.getUrl(bucketName,msgBody).getFile());

//                TODO check where should i reset the counter
                msg_to_workers_queue_counter = 0;
            }
            messages = sqs.receiveMessage(local_to_managerSQS).getMessages();


            msg_to_workers_queue_counter = 0;
        }
//        s3.getbucket
//        start manager etc
//        get local_to_manager queue
//        get manager_to_local queue
//        download the url file from s3
//        create a message for each url and counter++

//        create works accordingly

//        check works_to_manager que for messages
//        when a message "done OCR task" received

    }
}
