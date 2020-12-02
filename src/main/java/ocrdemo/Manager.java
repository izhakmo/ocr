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
        assert instances != null;
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

        System.out.println("managerID: "+ managerID);

        String local_to_managerSQS = sqs.getQueueUrl("local-to-manager-sqs" + managerID).getQueueUrl();

        String bucketName = "manager-bucket-"+managerID;

        List<Message> messages = sqs.receiveMessage(local_to_managerSQS).getMessages();
        int msg_to_manager_Counter = 1;
        int msg_to_workers_queue_counter = 0;


        String key_pair_string = "key"+new Date().getTime();
        CreateKeyPairRequest createKeyPairRequest = new CreateKeyPairRequest();
        createKeyPairRequest.withKeyName(key_pair_string);

        CreateKeyPairResult createKeyPairResult = ec2.createKeyPair(createKeyPairRequest);

        KeyPair keyPair = new KeyPair();
        keyPair = createKeyPairResult.getKeyPair();



        String manager_to_workers_queue = getManager_to_WorkerSQS(sqs,ec2);





        List<String> valuesT1 = new ArrayList<>();
        valuesT1.add("worker");
        List<String> valuesT2 = new ArrayList<>();
        valuesT2.add("running");
        valuesT2.add("pending");
        Filter filter_worker = new Filter("tag:worker", valuesT1);
        Filter filter_running = new Filter("instance-state-name",valuesT2);

        DescribeInstancesRequest request = new DescribeInstancesRequest().withFilters(filter_worker,filter_running);


        DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter_worker,filter_running));

        List<Reservation> reservations = result.getReservations();
        ArrayList<String> active_workersID = new ArrayList<>();


        for (Reservation reservation : reservations) {
            List<Instance> instances = reservation.getInstances();

            for (Instance instance : instances) {

                System.out.println("instance.getInstanceId() "+instance.getInstanceId());
                active_workersID.add(instance.getInstanceId());

            }
        }

        int number_of_active_workers = active_workersID.size();
        System.out.println("number_of_active_workers: "+number_of_active_workers);


        Tag worker_tag = new Tag();
        worker_tag.setKey("worker");
        worker_tag.setValue("worker");

        CreateTagsRequest tagsRequest = new CreateTagsRequest().withTags(worker_tag);

        tagsRequest.withTags(worker_tag);

        TagSpecification tag_specification = new TagSpecification();



        while (! messages.isEmpty()) {
            while (! messages.isEmpty()) {




                Message msg = messages.remove(0);

                String msgBody = msg.getBody();
                String [] msg_splitted =msgBody.split(" ");
                String number_of_messages_per_worker = msg_splitted[1];
                GetObjectRequest object_request = new GetObjectRequest(bucketName,msgBody);

                System.out.println("number_of_messages_per_worker: "+number_of_messages_per_worker);


                S3Object o = s3.getObject(object_request);
                S3ObjectInputStream object_content = o.getObjectContent();
                System.out.println(msg_to_manager_Counter+". msg_to_manager_Counter");
//                displayTextInputStream(object_content);


                BufferedReader reader = new BufferedReader(new InputStreamReader(object_content));
                String line = null;


//                send the url messages ang count them
                while ((line = reader.readLine()) != null) {
                    if(line.length()==0)    {  continue; }

                    System.out.println(line);
                    SendMessageRequest url_msg_request_to_worker = new SendMessageRequest()
                            .withQueueUrl(manager_to_workers_queue)
                            .withMessageBody(line);

                    sqs.sendMessage(url_msg_request_to_worker);

                    msg_to_workers_queue_counter++;

                }
                int number_of_workers_needed_for_task = (msg_to_workers_queue_counter/ Integer.parseInt(number_of_messages_per_worker))+1;
                System.out.println(msg_to_manager_Counter+ ". number_of_workers_needed_for_task: "+ number_of_workers_needed_for_task);



//                delete local_to_manager message
                sqs.deleteMessage(local_to_managerSQS,msg.getReceiptHandle());


                int number_of_workers_to_create = number_of_workers_needed_for_task - number_of_active_workers;


                number_of_workers_to_create = Math.max(number_of_workers_to_create, 0);

//                19 is the maximum if ec2 instances allowed in aws student permission
                if((number_of_workers_to_create + number_of_active_workers) > 17){
                    number_of_workers_to_create = 18 - number_of_active_workers;
                    number_of_active_workers = 18;

                }
                else{
                    number_of_active_workers += number_of_workers_to_create;
                }



                System.out.println("number_of_workers_to_create: "+ number_of_workers_to_create);

                if(number_of_workers_to_create >0) {


                    RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
                    runInstancesRequest.withImageId("ami-04d29b6f966df1537")
                            .withInstanceType(InstanceType.T2Micro)
                            .withMinCount(number_of_workers_to_create).withMaxCount(number_of_workers_to_create)
                            .withKeyName(key_pair_string)  //TODO ?????
                            .withSecurityGroupIds("sg-0d23010af4dee7fa3")
                            .withTagSpecifications(tag_specification);


                    RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);
                    List<Instance> worker_instances = runInstancesResult.getReservation().getInstances();


                    ArrayList<String> workersID = new ArrayList<>();
                    int number_of_workers_to_print = 1;
                    while (!worker_instances.isEmpty()) {
                        workersID.add(worker_instances.remove(0).getInstanceId());

//                        workersID.add(worker_instances.get(0).getInstanceId());

                        System.out.println("number_of_workers_to_print: " + number_of_workers_to_print);
                        number_of_workers_to_print++;


                    }

                    CreateTagsRequest tag_request = new CreateTagsRequest()
                            .withTags(worker_tag)
                            .withResources(workersID);

                    CreateTagsResult tag_response = ec2.createTags(tag_request);
                }   //end of if - run_workers


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
