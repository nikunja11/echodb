package com.echodb.storage;

import com.echodb.config.EchoDBConfig;
import com.echodb.exception.EchoDBException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.*;

import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import java.net.URI;

import java.util.Optional;
import java.util.List;
import java.util.ArrayList;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * S3 storage manager for SlateDB
 */
public class S3StorageManager {
    private final EchoDBConfig config;
    private final S3Client s3Client;
    private final String bucketName;
    
    public S3StorageManager(EchoDBConfig config) {
        this.config = config;
        
        // ✅ Configure for LocalStack if S3_ENDPOINT env var is set
        String s3Endpoint = System.getenv("S3_ENDPOINT");
        
        S3ClientBuilder builder = S3Client.builder()
            .region(Region.of(config.getS3Region()));
        
        if (s3Endpoint != null) {
            // LocalStack configuration
            builder.endpointOverride(URI.create(s3Endpoint))
                   .credentialsProvider(StaticCredentialsProvider.create(
                       AwsBasicCredentials.create("test", "test")))
                   .forcePathStyle(true);
        }
        
        this.s3Client = builder.build();
        this.bucketName = config.getS3Bucket();
    }
    
    public void put(String key, byte[] data) throws EchoDBException {
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                .bucket(config.getS3Bucket())
                .key(key)
                .build();
            
            s3Client.putObject(request, RequestBody.fromBytes(data));
        } catch (Exception e) {
            System.out.println("Failed to put object to S3: " + key+ " Exception :"+e);
            throw new EchoDBException("Failed to put object to S3: " + key, e);
        }
    }
    
    public Optional<byte[]> get(String key) throws EchoDBException {
    //public Optional<byte[]> get(String key) {
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                .bucket(config.getS3Bucket())
                .key(key)
                .build();
            
            return Optional.of(s3Client.getObject(request).readAllBytes());
        } catch (NoSuchKeyException e) {
            return Optional.empty();
        } catch (Exception e) {
            throw new EchoDBException("Failed to get object from S3: " + key, e);
            //return Optional.empty();
        }
    }
    
    public void delete(String key) throws EchoDBException {
        try {
            DeleteObjectRequest request = DeleteObjectRequest.builder()
                .bucket(config.getS3Bucket())
                .key(key)
                .build();
            
            s3Client.deleteObject(request);
        } catch (Exception e) {
            throw new EchoDBException("Failed to delete object from S3: " + key, e);
        }
    }
    
    public void close() {
        s3Client.close();
    }
    
    /**
     * List all files with the given prefix
     */
    public List<String> listFiles(String prefix) throws EchoDBException {
        try {
            List<String> files = new ArrayList<>();
            
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .build();
                
            ListObjectsV2Response response = s3Client.listObjectsV2(request);
            
            for (S3Object object : response.contents()) {
                files.add(object.key());
            }
            
            return files;
            
        } catch (Exception e) {
            throw new EchoDBException("Failed to list files with prefix: " + prefix, e);
        }
    }
    
    /**
     * List WAL files specifically
     */
    public List<String> listWALFiles() throws EchoDBException {
        return listFiles("wal/");
    }
    
    /**
     * Check if a key exists in S3
     */
    public boolean exists(String key) throws EchoDBException {
        try {
            HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(config.getS3Bucket())
                .key(key)
                .build();
            
            s3Client.headObject(request);
            return true;
            
        } catch (NoSuchKeyException e) {
            return false;
        } catch (Exception e) {
            throw new EchoDBException("Failed to check if object exists in S3: " + key, e);
        }
    }
}
