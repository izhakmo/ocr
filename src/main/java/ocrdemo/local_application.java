package ocrdemo;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class local_application {

    private static S3Client s3;
    public static void main(String[] args){
        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();
    }
}
