package org.adrian;

import java.io.File;
import java.io.IOException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

public class Uploader {

    public static void main(String[] args) throws InterruptedException, IOException {

        //AWSCredentials credentials = new BasicAWSCredentials("","");
        //ClientConfiguration config = new ClientConfiguration()
        //                                    .withProxyHost("myproxy")
        //                                    .withProxyPort(8080);

        upload(credentials, config);
    }


    private static void upload(AWSCredentials credentials, ClientConfiguration config) throws InterruptedException, IOException {
        String filename = "e:\\data\\shak.txt";
        String existingBucketName = "poobar";
        String keyName = "shakespeare.txt";
        File file = new File(filename);

        AmazonS3Client conn = new AmazonS3Client(credentials, config);
        //conn.putObject(new PutObjectRequest(existingBucketName, keyName, file));
        PutObjectRequest request = new PutObjectRequest(existingBucketName, keyName, file);

        TransferManager tm = new TransferManager(conn);
        //TransferManagerConfiguration tfc = new TransferManagerConfiguration();
        //System.err.println(tfc.getMinimumUploadPartSize());
        //tfc.setMinimumUploadPartSize(655360);
        //tfc.setMultipartUploadThreshold(1024);
        //tm.setConfiguration(tfc);

        request.setGeneralProgressListener(new ProgressListener() {
            public void progressChanged(ProgressEvent event) {
                if (event.getEventCode() > 0)
                    System.out.println("Transferred bytes: " +
                        event.getBytesTransferred() + " Code: " +  event.getEventCode());
            }
        });

        Upload upload = tm.upload(request);

        try {
            upload.waitForCompletion();
        } catch (AmazonClientException amazonClientException) {
            System.out.println("Unable to upload file, upload was aborted.");
            amazonClientException.printStackTrace();
        }


    }

}
