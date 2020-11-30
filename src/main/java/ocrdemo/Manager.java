package ocrdemo;


import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;

import java.io.*;
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
        int msgCounter = 1;
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
                System.out.println(msgCounter+". \n\n"+" ====================================\n\n");
                displayTextInputStream(object_content);


                System.out.println(msgCounter + ". msgBody: " + msgBody);
                msgCounter++;

//                File download_file_from_s3 =  new File(s3.getUrl(bucketName,msgBody).getFile());
                System.out.println(msgCounter + ". File: "+ s3.getUrl(bucketName,msgBody).getFile());

            }
            messages = sqs.receiveMessage(local_to_managerSQS).getMessages();
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
