package com.tbdhealth.parquetdemo;

import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3Uploader {

    private static final Logger log = LoggerFactory.getLogger(S3Uploader.class);

    private final S3Client s3;
    private final String bucket;

    public S3Uploader(S3Client s3, String bucket) {
        this.s3 = s3;
        this.bucket = bucket;
    }

    public void upload(Path localFile, String s3Key) {
        log.info("Uploading {} -> s3://{}/{}", localFile.getFileName(), bucket, s3Key);
        s3.putObject(
            PutObjectRequest.builder().bucket(bucket).key(s3Key).build(), localFile);
        log.info("Upload complete: s3://{}/{}", bucket, s3Key);
    }
}
