package com.scalyr.s3bench;

import com.amazonaws.services.s3.AmazonS3Client;

import org.apache.logging.log4j.Logger;

class TaskInfo
{
    /* all public because this is just a plain-old-data aggregate */
    public Logger logger;
    public AmazonS3Client s3;
    public String bucketName;
    public String operation;
    public int objectSize;
    public int partialSize;
    public int threadCount;
    public int version;

    public TaskInfo( Logger logger, AmazonS3Client s3, String bucketName, String operation, int objectSize, int threadCount, int version )
    {
        this.logger = logger;
        this.s3 = s3;
        this.bucketName = bucketName;
        this.operation = operation;
        this.objectSize = objectSize;
        this.partialSize = objectSize;
        this.threadCount = threadCount;
        this.version = version;
    }

    public void logResult( String objectId, long millisecondsElapsed, String errorMessage )
    {
        if ( errorMessage == null )
        {
            this.logger.error( "op=%s version=%d threads=%d size=%d objectSize=%d bucket=%s objectId=%s timeMs=%d",
                this.operation, this.version, this.threadCount, this.partialSize, this.objectSize, this.bucketName, objectId, millisecondsElapsed );
        }
        else
        {
            this.logger.error( "op=%s version=%d threads=%d size=%d objectSize=%d bucket=%s objectId=%s timeMs=%d error=[%s]",
                this.operation, this.version, this.threadCount, this.partialSize, this.objectSize, this.bucketName, objectId, millisecondsElapsed, errorMessage );
        }
    }

}

