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
import com.amazonaws.services.sqs.model.*;


import java.io.*;
import java.util.*;

public class local_application {


    public static String GetManager(AmazonEC2 ec2){
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


    public static AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
    public static AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();
    public static AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

    public static void main(String[] args) throws IOException {

//        key pair request

        String key_pair_string = "key"+new Date().getTime();
        CreateKeyPairRequest createKeyPairRequest = new CreateKeyPairRequest();
        createKeyPairRequest.withKeyName(key_pair_string);


        String managerID = GetManager(ec2);
        String local_to_managerSQS;



        CreateKeyPairResult createKeyPairResult = ec2.createKeyPair(createKeyPairRequest);

        KeyPair keyPair;
        keyPair = createKeyPairResult.getKeyPair();

//        String privateKey = keyPair.getKeyMaterial();
        keyPair.getKeyMaterial();


        //TODO check if a manager instance is active
        if(managerID == null) {
            Tag tag = new Tag();
            tag.setKey("manager");
            tag.setValue("manager");
            CreateTagsRequest tagsRequest = new CreateTagsRequest().withTags(tag);

            tagsRequest.withTags(tag);

            TagSpecification tag_specification = new TagSpecification();

            IamInstanceProfileSpecification spec = new IamInstanceProfileSpecification()
                    .withName("worker_and_sons");

//            RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
//            runInstancesRequest.withImageId("ami-04d29b6f966df1537")
//                    .withInstanceType(InstanceType.T2Micro)
//                    .withMinCount(1).withMaxCount(1)
//                    .withKeyName(key_pair_string)  //TODO ?????
//                    .withSecurityGroupIds("sg-0d23010af4dee7fa3")
//                    .withTagSpecifications(tag_specification);


//                    TODO this is not a todo
//                      its omer's image and securityGroups
            RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
            runInstancesRequest.withImageId("ami-0b43de3e8b3bb4e5d")
                    .withInstanceType(InstanceType.T2Micro)
                    .withMinCount(1).withMaxCount(1)
                    .withKeyName(key_pair_string)  //TODO ?????
                    .withSecurityGroupIds("sg-0d23010af4dee7fa3")
                    .withTagSpecifications(tag_specification)
                    .withIamInstanceProfile(spec);


            RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);
            managerID = runInstancesResult.getReservation().getInstances().get(0).getInstanceId();


            CreateTagsRequest tag_request = new CreateTagsRequest()
                    .withTags(tag)
                    .withResources(managerID);

//            CreateTagsResult tag_response = ec2.createTags(tag_request);
            ec2.createTags(tag_request);

            local_to_managerSQS = sqs.createQueue("local-to-manager-sqs" + managerID).getQueueUrl();

            System.out.println("===================================== MANAGER CREATED =================================================");
        }
        else{
            local_to_managerSQS = sqs.getQueueUrl("local-to-manager-sqs" + managerID).getQueueUrl();
            System.out.println("local_to_managerSQS: "+ local_to_managerSQS);

            System.out.println("===================================== MANAGER IS ALREADY UP =====================================================");
        }
        String local_app_name = "izhak"+new Date().getTime();

        String bucket_name = "bucket-"+local_app_name;
        Bucket bucket =  s3.createBucket(bucket_name);



//        TODO so far the number below is defualt and not the real one
//        String number_of_tasks_per_worker =  "5";
        String number_of_tasks_per_worker =  args[5];

        // Upload a file as a new object with ContentType and title specified.
//        String local_app_name = "Izhak1607356236606";

        String manager_to_localSQS = sqs.createQueue("manager-to-local-sqs" + local_app_name)
                .getQueueUrl();

        String optional_terminate = (args.length > 6 && args[6].equals("terminate")) ? "terminate" : "." ;

        String file_to_upload = "fileObjKeyName" + new Date().getTime() +" "+number_of_tasks_per_worker + " "
                + local_app_name + " " + optional_terminate;
//        String path = "C:\\Users\\izhak\\IdeaProjects\\text.images.txt";     //TODO need to be args[0]
        String path = args[3];



        PutObjectRequest putObjectRequest = new PutObjectRequest(bucket.getName(),file_to_upload , new File(path));
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("moshe Type");
        metadata.addUserMetadata("title", "Amir.Tal rock hard");
        putObjectRequest.setMetadata(metadata);
        s3.putObject(putObjectRequest);

//    } catch (AmazonServiceException e) {
//        // The call was transmitted successfully, but Amazon S3 couldn't process
//        // it, so it returned an error response.
//        e.printStackTrace();
//    } catch (SdkClientException e) {
//        // Amazon S3 couldn't be contacted for a response, or the client
//        // couldn't parse the response from Amazon S3.
//        e.printStackTrace();
//    }



        System.out.println("local_to_managerSQS: "+ local_to_managerSQS);



        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(local_to_managerSQS)
                .withMessageBody(file_to_upload);

        sqs.sendMessage(send_msg_request);


//        wait until we get a msg and then process it
        ReceiveMessageRequest task_is_done_msg_request = new ReceiveMessageRequest(manager_to_localSQS);
        task_is_done_msg_request.withWaitTimeSeconds(20);

        List<Message> messages = sqs.receiveMessage(task_is_done_msg_request).getMessages();
        int msg_not_receive_counter = 1;

//        task is not done yet
        while(messages.size() == 0 ){
            messages = sqs.receiveMessage(task_is_done_msg_request).getMessages();
            System.out.println("msg_not_receive_counter: "+ msg_not_receive_counter);
            msg_not_receive_counter++;

        }

//        String output_file_name = "output" + new Date().getTime() + ".html";
        String output_file_name = args[4] + ".html";
        int end_of_name = output_file_name.length()-5;

        File output_file = new File(output_file_name);
        FileWriter fileWriter = new FileWriter(output_file_name);
        fileWriter.write("<html>\n<head>\n<title>" + output_file_name.substring(0,end_of_name) + "  </title>\n</head>\n<body>\n");

        Message msg =  messages.remove(0);
//        ListObjectsV2Result texts = s3.listObjectsV2(bucket_name+ "/"+local_app_name);
        ListObjectsV2Result texts = s3.listObjectsV2(bucket_name,local_app_name+ "/");
        List<S3ObjectSummary> summaryList = texts.getObjectSummaries();
        summaryList = summaryList.subList(1, summaryList.size());

        for (S3ObjectSummary summary: summaryList) {
            String key = summary.getKey();
            System.out.println("summary list key : " + key);


            try {
                S3Object o = s3.getObject(bucket_name, key);

                S3ObjectInputStream object_content = o.getObjectContent();
                System.out.println("object received ");

                BufferedReader reader = new BufferedReader(new InputStreamReader(object_content));

                String line;

                int start_of_url = local_app_name.length()+1;
                int end_of_url = key.length()-4;
                fileWriter.write("<p>\n<img src=\""+key.substring(start_of_url,end_of_url) + "\">\n");
//                StringBuilder object_body= new StringBuilder();
//                int bodies = 1;
                while ((line = reader.readLine()) != null){
//                    object_body.append(line).append("\n");
//                    object_body.append("\n");
                    fileWriter.write("<br>" + line);


//                    System.out.println("number of while loops : " + bodies);
//                    bodies++;
                }



                fileWriter.write("</p>\n");


            }
            catch (Exception e){
                e.printStackTrace();
            }



        }

        fileWriter.write("</body>\n</html>");
        fileWriter.close();



//        if(args.length > 6 && args[6].equals("terminate")) {
//            SendMessageRequest terminate_request = new SendMessageRequest()
//                    .withQueueUrl(local_to_managerSQS)
//                    .withMessageBody("terminate");
//
//            sqs.sendMessage(terminate_request);
//        }
//            //TODO terminate manager at EC2
//        }

//            TODO check Qs - open manager if we no manager is up ==> tags
//            TODO response
//            TODO terminate works etc .....




//
        sqs.deleteMessage(manager_to_localSQS,msg.getReceiptHandle());
        sqs.deleteQueue(manager_to_localSQS);
    }

}
