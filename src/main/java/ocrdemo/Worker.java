package ocrdemo;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import net.sourceforge.tess4j.ITesseract;
import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.net.URL;

import java.util.List;


public class Worker {
    public static void main(String[] args) throws IOException {
        AmazonEC2 ec2 = local_application.ec2;
        AmazonS3 s3 = local_application.s3;
        AmazonSQS sqs = local_application.sqs;
        String managerID = local_application.GetManager(ec2);
        String manager_to_workers_queue = Manager.getManager_to_WorkerSQS(sqs, ec2);
        String worker_to_manager_queue = Manager.getWorker_to_ManagerSQS(sqs,ec2);
//        String workerID = Manager.getWorker(ec2);
        List<Message> messages =sqs.receiveMessage(manager_to_workers_queue).getMessages();
        ITesseract img_ocr_object = new Tesseract();


        int counter = 1;
        int ocr_fail_counter=1;

        while (!messages.isEmpty()) {
            while (!messages.isEmpty()) {
                Message msg = messages.remove(0);


//                if (msg.getBody().equals("terminate")) {
//
//                    sqs.deleteMessage(manager_to_workers_queue, msg.getReceiptHandle());
//
//
//
//                    SendMessageRequest terminate_msg = new SendMessageRequest()
//                            .withQueueUrl(worker_to_manager_queue)
//                            .withMessageBody( "terminated" );
//
//                    sqs.sendMessage(terminate_msg);
//
//
//                    System.exit(0);
//                }
//
//                else {
                    String[] msgBody = msg.getBody().split(" ");
                    String folder_name = msgBody[0];
                    String url = msgBody[1];
                    String url_number = msgBody[2];

                    System.out.println(counter + ". msgBody: " + url);
                    System.out.println(counter + ". folder_name: " + folder_name);


                    counter++;

                    String file_to_upload_to_s3 = url_number + "_" + url.substring(url.lastIndexOf('/') + 1);

                    File file = new File(file_to_upload_to_s3);
                    System.out.println("file created" + file_to_upload_to_s3);
                    String ocr_output;
                    try {
                        FileUtils.copyURLToFile(new URL(url), file, 2000, 2000);
//                    try {
                        ocr_output = img_ocr_object.doOCR(file);
                    } catch (TesseractException e) {
                        System.out.println(ocr_fail_counter + ". ocr failed");
                        ocr_fail_counter++;

                        ocr_output = url + ": ocr failed";
                    } catch (Exception e) {
                        ocr_output = url + ": broken or illegal url";
                    }
                    if (ocr_output != null) {
                        String path_s3 = folder_name + "/" + file_to_upload_to_s3;

                        String txt_file_name = file_to_upload_to_s3 + ".txt";
                        String txt_path_s3 = folder_name + "/" + url + ".txt";
                        File txt_file = new File(txt_file_name);
                        FileWriter file_writer = new FileWriter(txt_file_name);
                        file_writer.write(ocr_output);
                        file_writer.close();

                        String bucketName = "bucket-" + folder_name;



                        System.out.println("path_s3: " + path_s3 + ".");
//                        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName ,path_s3 , file);
                        PutObjectRequest putObjectRequest_txt = new PutObjectRequest(bucketName, txt_path_s3, txt_file);

//                        ObjectMetadata metadata = new ObjectMetadata();
//                        metadata.addUserMetadata("OCR", ocr_output);
//                        putObjectRequest.setMetadata(metadata);
//                        s3.putObject(putObjectRequest);
                        s3.putObject(putObjectRequest_txt);

                        System.out.println(url_number + ". ocr_output:" + ocr_output);
                        SendMessageRequest send_msg_request = new SendMessageRequest()
                                .withQueueUrl(worker_to_manager_queue)
                                .withMessageBody(folder_name);

                        sqs.sendMessage(send_msg_request);

                        txt_file.delete();


                    }
//                }
//                catch (Exception e){
//                    System.out.println("illegal url : " +  url );
//
//
//
//
//                }


//                Image_reader reader = new Image_reader(msgBody[0], msgBody[1]);
//                String answer = reader.OCR();
//                String answer = "reader.OCR()";
//                SendMessageRequest send_msg_request = new SendMessageRequest()
//                        .withQueueUrl(worker_to_manager_queue)
//                        .withMessageBody(answer);
//
//               sqs.sendMessage(send_msg_request);
                    sqs.deleteMessage(manager_to_workers_queue, msg.getReceiptHandle());
                    file.delete();

//                } else
//                terminate msg


                messages = sqs.receiveMessage(manager_to_workers_queue).getMessages();
            }
        }
    }
}
