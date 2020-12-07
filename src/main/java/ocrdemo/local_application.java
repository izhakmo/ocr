package ocrdemo;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
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
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;


import java.io.*;
import java.util.*;

public class local_application {


    public static String GetManager(AmazonEC2 ec2){
        String manager = null;
//
//        if(reservations!=null){
//            System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
//            Reservation reservation = reservations.get(0);
//            List<Instance> instances = reservation.getInstances();
//            manager = instances.get(0);
//        }

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
//        s3 = AmazonS3ClientBuilder.defaultClient();

//        key pair request

        String key_pair_string = "key"+new Date().getTime();
        CreateKeyPairRequest createKeyPairRequest = new CreateKeyPairRequest();
        createKeyPairRequest.withKeyName(key_pair_string);


        String managerID = GetManager(ec2);
        String local_to_managerSQS;



        CreateKeyPairResult createKeyPairResult = ec2.createKeyPair(createKeyPairRequest);

        KeyPair keyPair = new KeyPair();
        keyPair = createKeyPairResult.getKeyPair();

        String privateKey = keyPair.getKeyMaterial();


        //TODO check if a manager instance is active
        if(managerID == null) {
            Tag tag = new Tag();
            tag.setKey("manager");
            tag.setValue("manager");
            CreateTagsRequest tagsRequest = new CreateTagsRequest().withTags(tag);

            tagsRequest.withTags(tag);

            TagSpecification tag_specification = new TagSpecification();


            RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
            runInstancesRequest.withImageId("ami-04d29b6f966df1537")
                    .withInstanceType(InstanceType.T2Micro)
                    .withMinCount(1).withMaxCount(1)
                    .withKeyName(key_pair_string)  //TODO ?????
                    .withSecurityGroupIds("sg-0d23010af4dee7fa3")
                    .withTagSpecifications(tag_specification);


            RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);
            managerID = runInstancesResult.getReservation().getInstances().get(0).getInstanceId();


            CreateTagsRequest tag_request = new CreateTagsRequest()
                    .withTags(tag)
                    .withResources(managerID);

            CreateTagsResult tag_response = ec2.createTags(tag_request);

            local_to_managerSQS = sqs.createQueue("local-to-manager-sqs" + managerID).getQueueUrl();

            System.out.println("===================================== MANAGER CREATED =================================================");
        }
        else{
            local_to_managerSQS = sqs.getQueueUrl("local-to-manager-sqs" + managerID).getQueueUrl();
            System.out.println("local_to_managerSQS: "+ local_to_managerSQS);

            System.out.println("===================================== MANAGER IS ALREADY UP =====================================================");
        }

        String bucket_name = "manager-bucket-"+managerID;
        Bucket bucket =  s3.createBucket(bucket_name);



//        TODO so far the number below is defualt and not the real one
        String number_of_tasks_per_worker =  "5";

        // Upload a file as a new object with ContentType and title specified.
        String local_app_name = "Izhak"+new Date().getTime();

        String manager_to_localSQS = sqs.createQueue("manager-to-local-sqs" + local_app_name)
                .getQueueUrl();

        String file_to_upload = "fileObjKeyName" + new Date().getTime() +" "+number_of_tasks_per_worker + " " + local_app_name;
        String path = "C:\\Users\\izhak\\IdeaProjects\\text.images.txt";     //TODO need to be args[0]



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


//        TODO wait until we get a msg and then process it
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

//        TODO html
        System.out.println("============================== TODO html ==================================== ");
        String output_file_name = "output" + new Date().getTime() + ".html";
        int end_of_name = output_file_name.length()-5;

        File output_file = new File(output_file_name);
        FileWriter fileWriter = new FileWriter(output_file_name);
        fileWriter.write("<html>\n<head>\n<title>" + output_file_name.substring(0,end_of_name) + "  </title>\n</head>\n<body>\n");

        Message msg =  messages.remove(0);
        ListObjectsV2Result texts = s3.listObjectsV2(bucket_name);
        List<S3ObjectSummary> summaryList = texts.getObjectSummaries();
        for (S3ObjectSummary summary: summaryList) {
            String key = summary.getKey();
            System.out.println("summary list key : " + key);


            try {
                S3Object o = s3.getObject(bucket_name, key);

                S3ObjectInputStream object_content = o.getObjectContent();
                System.out.println("object received ");

                BufferedReader reader = new BufferedReader(new InputStreamReader(object_content));

                String line = null;

                fileWriter.write("<p>\n<img src=\""+key.substring(0,key.length()-4) + "\">\n");
                StringBuilder object_body= new StringBuilder();
                int bodies = 1;
                while ((line = reader.readLine()) != null){
                    object_body.append(line);
                    object_body.append("\n");
                    System.out.println("number of while loops : " + bodies);
                    bodies++;
                }



                fileWriter.write("<br>\n\"" + object_body + "\"\n</p>\n");


            }
            catch (Exception e){
                e.printStackTrace();
            }



        }

        fileWriter.write("</body>\n</html>");
        fileWriter.close();


//        GetObjectRequest object_request = new GetObjectRequest(bucket_name,messages.get(0).getBody());
//
//        S3Object o = s3.getObject(object_request);
//        S3ObjectInputStream object_content = o.getObjectContent();


//        Map<String,String> attributes =  messages.get(0).getAttributes();
//        if (attributes.containsKey("done task")){



        if(args.length > 4 && args[4].equals("terminate")) {
            SendMessageRequest terminate_request = new SendMessageRequest()
                    .withQueueUrl(local_to_managerSQS)
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
