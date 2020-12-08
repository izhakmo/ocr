package ocrdemo;


import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import java.io.*;
import java.util.*;

public class Manager {


    public static String getManager(AmazonEC2 ec2){
        String manager = null;


//      continue with key after we got the manager


        List<String> valuesT1 = new ArrayList<>();
        valuesT1.add("manager");
        List<String> valuesT2 = new ArrayList<>();
        valuesT2.add("running");
        valuesT2.add("pending");
        Filter filter_manager = new Filter("tag:manager", valuesT1);
        Filter filter_running = new Filter("instance-state-name",valuesT2);
//        Filter filter_running = new Filter("tag:manager", valuesT1);

        DescribeInstancesRequest request = new DescribeInstancesRequest().withFilters(filter_manager,filter_running);


        DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter_manager,filter_running));

        List<Reservation> reservations = result.getReservations();

        for (Reservation reservation : reservations) {
            List<Instance> instances = reservation.getInstances();

            for (Instance instance : instances) {

//                System.out.println(instance.getInstanceId());
                manager = instance.getInstanceId();
                System.out.println("manager: "+ manager);


            }
        }
        return manager;
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

    public static List<String>getWorkers_list(AmazonEC2 ec2) {
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

        return active_workersID;
    }

    public static String getManager_to_WorkerSQS(AmazonSQS sqs, AmazonEC2 ec2,String managerID){
        String manager_to_workers_queue;
        try {
            manager_to_workers_queue = sqs.getQueueUrl("manager"+managerID + "_to_workers").getQueueUrl();
        }
        catch (QueueDoesNotExistException e){
            manager_to_workers_queue = sqs.createQueue("manager"+managerID + "_to_workers").getQueueUrl();
        }
        return manager_to_workers_queue;
    }

    public static String getWorker_to_ManagerSQS(AmazonSQS sqs, AmazonEC2 ec2,String managerID){
        String worker_to_manager_queue;
        try {
            worker_to_manager_queue = sqs.getQueueUrl("worker_to_manager" + managerID).getQueueUrl();
        }
        catch (QueueDoesNotExistException e){
            worker_to_manager_queue = sqs.createQueue("worker_to_manager" + managerID).getQueueUrl();
        }
        return worker_to_manager_queue;
    }

    public static String getManager_to_Local_appSQS(AmazonSQS sqs, String local_app_name){
        return sqs.getQueueUrl("manager-to-local-sqs" + local_app_name).getQueueUrl();
    }

    public void send_done_task_msg_to_local_app(String local_app_name){

    }

    public static void createFolder(String bucketName, String folderName, AmazonS3 client) {
        // create meta-data for your folder and set content-length to 0
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);

        // create empty content
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

        // create a PutObjectRequest passing the folder name suffixed by /
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
                folderName + "/", emptyContent, metadata);

        // send request to S3 to create folder
        client.putObject(putObjectRequest);
    }

    public static void main(String[] args) throws IOException {
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();
        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        String managerID = getManager(ec2);
        Map<String,Integer> tasks_map = new HashMap<>();

        boolean terminate = false;




        String local_to_managerSQS = sqs.getQueueUrl("local-to-manager-sqs" + managerID).getQueueUrl();

        String worker_to_managerSQS = getWorker_to_ManagerSQS(sqs,ec2,managerID);


        List<Message> messages_from_local;
        List<Message> messages_from_workers;
        int msg_to_manager_Counter = 1;
        int msg_to_workers_queue_counter = 0;


        String key_pair_string = "key"+new Date().getTime();
        CreateKeyPairRequest createKeyPairRequest = new CreateKeyPairRequest();
        createKeyPairRequest.withKeyName(key_pair_string);

        CreateKeyPairResult createKeyPairResult = ec2.createKeyPair(createKeyPairRequest);

        KeyPair keyPair = new KeyPair();
        keyPair = createKeyPairResult.getKeyPair();



        String manager_to_workers_queue = getManager_to_WorkerSQS(sqs,ec2,managerID);



        List<String> active_workersID = getWorkers_list(ec2);


        int number_of_active_workers = active_workersID.size();




        System.out.println("number_of_active_workers: "+number_of_active_workers);


        Tag worker_tag = new Tag();
        worker_tag.setKey("worker");
        worker_tag.setValue("worker");


        CreateTagsRequest tagsRequest = new CreateTagsRequest().withTags(worker_tag);
        tagsRequest.withTags(worker_tag);

        TagSpecification tag_specification = new TagSpecification();


        while(true) {
            try {
                messages_from_local = sqs.receiveMessage(local_to_managerSQS).getMessages();
            }
            catch (QueueDoesNotExistException e){
                messages_from_local = null;

            }
            if(messages_from_local!= null) {
                while (!messages_from_local.isEmpty()) {
                    while (!messages_from_local.isEmpty()) {


                        Message msg = messages_from_local.remove(0);

                        String msgBody = msg.getBody();
                        String[] msg_splitted = msgBody.split(" ");


                        String number_of_messages_per_worker = msg_splitted[1];
                        String local_app_name = msg_splitted[2];
                        String optional_terminate = msg_splitted[3];
                        if (optional_terminate.equals("terminate")) {
                            terminate = true;

                            sqs.deleteQueue(local_to_managerSQS);


                        }

                        String bucketName = "bucket-" + local_app_name;


                        createFolder(bucketName, local_app_name, s3);

                        int url_number = 0;
                        GetObjectRequest object_request = new GetObjectRequest(bucketName, msgBody);

                        System.out.println("number_of_messages_per_worker: " + number_of_messages_per_worker);


                        S3Object o = s3.getObject(object_request);
                        S3ObjectInputStream object_content = o.getObjectContent();
                        System.out.println(msg_to_manager_Counter + ". msg_to_manager_Counter");
//                displayTextInputStream(object_content);


                        BufferedReader reader = new BufferedReader(new InputStreamReader(object_content));
                        String line;


//                send the url messages and count them
                        while ((line = reader.readLine()) != null) {
                            if (line.length() == 0) {
                                continue;
                            }

//                    System.out.println(line);
                            SendMessageRequest url_msg_request_to_worker = new SendMessageRequest()
                                    .withQueueUrl(manager_to_workers_queue)
                                    .withMessageBody(local_app_name + " " + line + " " + url_number);

                            sqs.sendMessage(url_msg_request_to_worker);

                            url_number++;
                            msg_to_workers_queue_counter++;

                        }

                        tasks_map.put(local_app_name, url_number);
                        System.out.println("task added to map :\n key- " + local_app_name + "\nvalue- " + url_number);


                        int number_of_workers_needed_for_task = (msg_to_workers_queue_counter / Integer.parseInt(number_of_messages_per_worker)) + 1;
                        System.out.println(msg_to_manager_Counter + ". number_of_workers_needed_for_task: " + number_of_workers_needed_for_task);


//                delete local_to_manager message
                        sqs.deleteMessage(local_to_managerSQS, msg.getReceiptHandle());


                        int number_of_workers_to_create = number_of_workers_needed_for_task - number_of_active_workers;


                        number_of_workers_to_create = Math.max(number_of_workers_to_create, 0);

//                19 is the maximum if ec2 instances allowed in aws student permission
                        if ((number_of_workers_to_create + number_of_active_workers) > 17) {
                            number_of_workers_to_create = 18 - number_of_active_workers;
                            number_of_active_workers = 18;

                        } else {
                            number_of_active_workers += number_of_workers_to_create;
                        }


                        System.out.println("number_of_workers_to_create: " + number_of_workers_to_create);

                        if (number_of_workers_to_create > 0) {

                            IamInstanceProfileSpecification spec = new IamInstanceProfileSpecification()
                                    .withName("worker_and_sons");

//                    RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
//                    runInstancesRequest.withImageId("ami-04d29b6f966df1537")
//                            .withInstanceType(InstanceType.T2Micro)
//                            .withMinCount(number_of_workers_to_create).withMaxCount(number_of_workers_to_create)
//                            .withKeyName(key_pair_string)  //TODO ?????
//                            .withSecurityGroupIds("sg-0d23010af4dee7fa3")
//                            .withTagSpecifications(tag_specification);


//                    TODO this is not a todo
//                      its omer's image and securityGroups
                            RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
                            runInstancesRequest.withImageId("ami-0b43de3e8b3bb4e5d")
                                    .withInstanceType(InstanceType.T2Micro)
                                    .withMinCount(number_of_workers_to_create).withMaxCount(number_of_workers_to_create)
                                    .withKeyName(key_pair_string)  //TODO ?????
                                    .withSecurityGroupIds("sg-0d23010af4dee7fa3")
                                    .withTagSpecifications(tag_specification)
                                    .withIamInstanceProfile(spec);


                            RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);
                            List<Instance> worker_instances = runInstancesResult.getReservation().getInstances();

                            //TODO make it a function called get workers ID as array
                            ArrayList<String> workersID = new ArrayList<>();
//                        int number_of_workers_to_print = 1;
                            while (!worker_instances.isEmpty()) {
                                workersID.add(worker_instances.remove(0).getInstanceId());

//                        workersID.add(worker_instances.get(0).getInstanceId());

//                            System.out.println("number_of_workers_to_print: " + number_of_workers_to_print);
//                            number_of_workers_to_print++;

                            }

                            CreateTagsRequest tag_request = new CreateTagsRequest()
                                    .withTags(worker_tag)
                                    .withResources(workersID);

//                    try until success
                            do {
                                try {
                                    CreateTagsResult tag_response = ec2.createTags(tag_request);
                                } catch (AmazonEC2Exception e) {
                                    System.out.println("AmazonEC2Exception occurred while trying to tag the workers");
                                    continue;
                                }
                                break;
                            } while (true);

                        }   //end of if - run_workers


                        System.out.println("msg_to_workers_queue_counter: " + msg_to_workers_queue_counter);


                        System.out.println(msg_to_manager_Counter + ". msgBody: " + msgBody);
                        msg_to_manager_Counter++;

//                File download_file_from_s3 =  new File(s3.getUrl(bucketName,msgBody).getFile());
                        System.out.println(msg_to_manager_Counter + ". File: " + s3.getUrl(bucketName, msgBody).getFile());

//                TODO check where should i reset the counter
                        msg_to_workers_queue_counter = 0;

                        if (terminate) {
                            break;
                        }

                    } // first while != null loop


                    if (terminate) {
                        break;
                    }

//                    TODO check that it doesnt brake the program
                    messages_from_local = sqs.receiveMessage(local_to_managerSQS).getMessages();


                    msg_to_workers_queue_counter = 0;

                }   // second while != null loop
            }

//            System.out.println("out of local app loops");


//        listening to workers LOOP
            boolean finish = false;
            while (!finish) {

                messages_from_workers = sqs.receiveMessage(worker_to_managerSQS).getMessages();
                while (!messages_from_workers.isEmpty()) {
                    while (!messages_from_workers.isEmpty()) {
                        Message msg = messages_from_workers.remove(0);
                        String msgBody = msg.getBody();
                        String[] msg_splitted = msgBody.split(" ");
                        String key = msg_splitted[0];
                        Integer value = tasks_map.get(key);
                        if (value != null) {
                            value--;

                            if (value == 0) {
                                String manager_to_Local_appSQS = getManager_to_Local_appSQS(sqs, key);
                                SendMessageRequest task_is_done_request = new SendMessageRequest()
                                        .withQueueUrl(manager_to_Local_appSQS)
                                        .withMessageBody(key + " " + "is_done");

                                sqs.sendMessage(task_is_done_request);

                                tasks_map.remove(key);
                                System.out.println("task: " + key + " is done");
                                finish = true;


                            } else {
                                tasks_map.replace(key, value);
                                System.out.println("task: " + key + " new value - " + value);
                            }


                        } else {
                            System.out.println("value is null");
                        }


                        System.out.println("while loop worker_to_managerSQS \n value : " + value);

//                send DONE TASK msg to local

                        sqs.deleteMessage(worker_to_managerSQS, msg.getReceiptHandle());
                    }
                    messages_from_workers = sqs.receiveMessage(worker_to_managerSQS).getMessages();


                }

                if(tasks_map.isEmpty()){
                    break;
                }

            }
            if (terminate) {
                TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest(getWorkers_list(ec2));
                ec2.terminateInstances(terminateInstancesRequest);
                sqs.deleteQueue(local_to_managerSQS);
                sqs.deleteQueue(worker_to_managerSQS);
                sqs.deleteQueue(manager_to_workers_queue);

                break;


            }

        }

        ArrayList<String> manager_at_list = new ArrayList<>();
        manager_at_list.add(getManager(ec2));
        TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest(manager_at_list);
        System.out.println("manager is done");

        ec2.terminateInstances(terminateInstancesRequest);



//        check workers_to_manager que for messages
//        when a message "done OCR task" received

    }
}
