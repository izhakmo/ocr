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

public class Worker {
    public static void main(String[] args) throws IOException {
        AmazonEC2 ec2 = local_application.ec2;
        AmazonS3 s3 = local_application.s3;
        AmazonSQS sqs = local_application.sqs;
        String managerID = local_application.GetManager(ec2);
        String manager_to_workers_queue = Manager.getManager_to_WorkerSQS(sqs, ec2);
        String worker_to_manager_queue = Manager.getWorker_to_ManagerSQS(sqs,ec2);
        String workerID = Manager.getWorker(ec2);
        String bucketName = "manager-bucket-" + managerID;
        List<Message> messages =sqs.receiveMessage(manager_to_workers_queue).getMessages();


        while (!messages.isEmpty()) {
            while (!messages.isEmpty()) {
                Message msg = messages.remove(0);
                String msgBody = msg.getBody();
                Image_reader reader = new Image_reader(msgBody);
                String answer = reader.OCR();
                SendMessageRequest send_msg_request = new SendMessageRequest()
                        .withQueueUrl(worker_to_manager_queue)
                        .withMessageBody(answer);

               sqs.sendMessage(send_msg_request);

            }
        }
    }
}
