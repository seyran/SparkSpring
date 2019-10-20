package com.seyrancom.service;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.seyrancom.service.client.AmazonS3ClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

@Slf4j
@Service
public class AmazonS3Service {

    private static final String CONTENT_TYPE = "application/json";
    private AmazonS3 client;

    public AmazonS3Service() {
        initClient();
    }

    private void initClient() {
        this.client = AmazonS3ClientFactory.createClient();
    }

    public void putObject(String bucketName, String key, String value) {
        PutObjectRequest request = createRequest(bucketName, key, value.getBytes(StandardCharsets.UTF_8));
        putObject(request);
    }

    private void putObject(PutObjectRequest request) {
        try {
            client.putObject(request);
        } catch (AmazonServiceException e) {
            log.error("The call was transmitted successfully, but Amazon S3 couldn't process it, so it returned an error response", e);
        } catch (SdkClientException e) {
            log.error("Amazon S3 couldn't be contacted for a response, or the client couldn't parse the response from Amazon S3", e);
        }
    }

    private PutObjectRequest createRequest(String bucketName, String key, byte[] value) {
        ObjectMetadata metadata = createObjectMetadata(value.length);
        PutObjectRequest request = new PutObjectRequest(bucketName, key, new ByteArrayInputStream(value), metadata);
        request.setMetadata(metadata);
        return request;
    }

    private ObjectMetadata createObjectMetadata(int contentLength) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType(CONTENT_TYPE);
        metadata.setContentLength(contentLength);
        return metadata;
    }
}
