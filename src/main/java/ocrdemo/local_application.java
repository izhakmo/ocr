package ocrdemo;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

public class local_application {

    private static AmazonS3 s3;

    public static void main(String[] args) throws IOException {
        s3 = AmazonS3ClientBuilder.defaultClient();

        Bucket buck =  s3.createBucket("moshehagever");


//        // Upload a text string as a new object.
        s3.putObject("moshehagever", "stringObjKeyName", "Uploaded String Object");


        // Upload a file as a new object with ContentType and title specified.
        PutObjectRequest request = new PutObjectRequest("moshehagever", "fileObjKeyName", new File("C:\\Users\\izhak\\IdeaProjects\\text.images.txt"));
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("plain/text");
        metadata.addUserMetadata("title", "someTitle");
        request.setMetadata(metadata);
        s3.putObject(request);


    }

}
