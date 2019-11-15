package com.provectus.fds.it;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.util.Iterator;

/**
 * Helper class intended to completely remove a S3 bucket.
 *
 * Code derived from AWS example:
 * https://docs.aws.amazon.com/en_us/AmazonS3/latest/dev/delete-or-empty-bucket.html
 */
class BucketRemover {

    private final AmazonS3 s3Client;

    BucketRemover(String clientRegion) {
        s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(clientRegion)
                .build();
    }

    private boolean checkIfBucketExists(String bucketName) {
        return s3Client.doesBucketExistV2(bucketName);
    }

    void removeBucketWithRetries(String bucketName, int retries) {
        int i = 0;
        for ( ; checkIfBucketExists(bucketName) && i < retries; i++) {
            removeBucket(bucketName);
        }

        if (checkIfBucketExists(bucketName)) {
            System.out.printf("WARNING: Oh-oh... Despite of %d tries bucket '%s' was not removed properly\n", retries, bucketName);
        } else {
            System.out.printf("SUCCESS: Bucket '%s' was removed successfully after %d retries", bucketName, i);
        }
    }

    private void removeBucket(String bucketName) {

        // Delete all objects from the bucket. This is sufficient
        // for unversioned buckets. For versioned buckets, when you attempt to delete objects, Amazon S3 inserts
        // delete markers for all objects, but doesn't delete the object versions.
        // To delete objects from versioned buckets, delete all of the object versions before deleting
        // the bucket (see below for an example).
        ObjectListing objectListing = s3Client.listObjects(bucketName);
        while (true) {
            for (S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) {
                s3Client.deleteObject(bucketName, s3ObjectSummary.getKey());
            }

            // If the bucket contains many objects, the listObjects() call
            // might not return all of the objects in the first listing. Check to
            // see whether the listing was truncated. If so, retrieve the next page of objects
            // and delete them.
            if (objectListing.isTruncated()) {
                objectListing = s3Client.listNextBatchOfObjects(objectListing);
            } else {
                break;
            }
        }

        // Delete all object versions (required for versioned buckets).
        VersionListing versionList = s3Client.listVersions(new ListVersionsRequest().withBucketName(bucketName));
        while (true) {
            for (S3VersionSummary vs : versionList.getVersionSummaries()) {
                s3Client.deleteVersion(bucketName, vs.getKey(), vs.getVersionId());
            }

            if (versionList.isTruncated()) {
                versionList = s3Client.listNextBatchOfVersions(versionList);
            } else {
                break;
            }
        }

        // After all objects and object versions are deleted, delete the bucket.
        s3Client.deleteBucket(bucketName);
    }
}