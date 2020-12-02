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
import java.util.ArrayList;
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

    public static Tag createTags(String name, String value){
        Tag tag = new Tag();
        tag.setKey(name);
        tag.setValue(value);
        CreateTagsRequest tagsRequest = new CreateTagsRequest().withTags(tag);

        tagsRequest.withTags(tag);

        return tag;
    }

    public static KeyPair createKeyPair(String keyName, AmazonEC2 ec2){
        String key_pair_string = keyName+new Date().getTime();
        CreateKeyPairRequest createKeyPairRequest = new CreateKeyPairRequest();
        createKeyPairRequest.withKeyName(key_pair_string);

        CreateKeyPairResult createKeyPairResult = ec2.createKeyPair(createKeyPairRequest);

        KeyPair keyPair = new KeyPair();
        keyPair = createKeyPairResult.getKeyPair();

        return keyPair;
    }

    public static String getWorker(AmazonEC2 ec2) {
        String worker = null;
        List<String> valuesT1 = new ArrayList<>();
        valuesT1.add("worker");
        List<String> valuesT2 = new ArrayList<>();
        valuesT2.add("running");
        Filter filter_worker = new Filter("tag:worker", valuesT1);
        Filter filter_running = new Filter("instance-state-name", valuesT2);

        DescribeInstancesRequest request = new DescribeInstancesRequest().withFilters(filter_worker, filter_running);


        DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter_worker, filter_running));

        List<Reservation> reservations = result.getReservations();
        List<Instance> instances = null;
        for (Reservation reservation : reservations) {
            instances = reservation.getInstances();
        }
        if (!instances.isEmpty()) {
            worker = instances.remove(0).getInstanceId();
            return worker;
        } else {
            return null;
        }
    }

    public static String getManager_to_WorkerSQS(AmazonSQS sqs, AmazonEC2 ec2){
        String manager_to_workers_queue;
        try {
            manager_to_workers_queue = sqs.getQueueUrl("manager"+local_application.GetManager(ec2) + "_to_workers").getQueueUrl();
        }
        catch (QueueDoesNotExistException e){
            manager_to_workers_queue = sqs.createQueue("manager"+local_application.GetManager(ec2) + "_to_workers").getQueueUrl();
        }
        return manager_to_workers_queue;
    }

    public static String getWorker_to_ManagerSQS(AmazonSQS sqs, AmazonEC2 ec2){
        String worker_to_manager_queue;
        try {
            worker_to_manager_queue = sqs.getQueueUrl("worker_to_manager" + local_application.GetManager(ec2)).getQueueUrl();
        }
        catch (QueueDoesNotExistException e){
            worker_to_manager_queue = sqs.createQueue("worker_to_manager" + local_application.GetManager(ec2)).getQueueUrl();
        }
        return worker_to_manager_queue;
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

                KeyPair keyPair = createKeyPair("key", ec2);
                Tag tag = createTags("worker","worker");
                TagSpecification tag_specification = new TagSpecification();


                RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
                runInstancesRequest.withImageId("ami-04d29b6f966df1537")
                        .withInstanceType(InstanceType.T2Micro)
                        .withMinCount(number_of_workers).withMaxCount(number_of_workers)
                        .withKeyName(keyPair.getKeyName())  //TODO ?????
                        .withSecurityGroupIds("sg-4d22bd78")
                        .withTagSpecifications(tag_specification);


                RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);
                String workerID = runInstancesResult.getReservation().getInstances().get(0).getInstanceId();


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
