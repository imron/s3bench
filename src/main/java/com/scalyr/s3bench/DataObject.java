package com.scalyr.s3bench;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import com.scalyr.s3bench.TaskInfo;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

class DataObject
{
    private byte[] data;
    private ObjectMetadata metadata;
    private ByteArrayInputStream dataStream;

    DataObject( int objectSize )
    {
        this.data = new byte[objectSize];
    }

    public byte[] checksum( String bucketName, String objectId )
    {
        String check = bucketName + objectId;
        return check.getBytes( StandardCharsets.UTF_8 );
    }

    public void clear()
    {
        Arrays.fill( this.data, (byte)0 );
    }

    public void fillBuffer( Random random, String bucketName, String objectId )
    {
        byte[] check = checksum( bucketName, objectId );

        random.setSeed( Arrays.hashCode( check ) );

        int checkLength = check.length * 2;

        if ( checkLength < this.data.length )
        {
            int randomLength = this.data.length - checkLength;

            byte[] randomData = new byte[randomLength];
            random.nextBytes( randomData );

            ByteBuffer buffer = ByteBuffer.wrap( this.data );
            buffer.put( check );
            buffer.put( randomData );
            buffer.put( check );
        }
        else
        {
            clear();
        }
    }

    public int size()
    {
        return this.data.length;
    }

    public String write( TaskInfo taskInfo, String objectId )
    {
        String result = null;
        if ( this.metadata == null )
        {
            this.metadata = new ObjectMetadata();
            this.metadata.setContentLength( this.data.length );
        }

        if ( this.dataStream == null )
        {
            this.dataStream = new ByteArrayInputStream( this.data );
        }

        this.dataStream.reset();

        try
        {
            taskInfo.s3.putObject( taskInfo.bucketName, objectId, this.dataStream, this.metadata );
        }
        catch( AmazonServiceException e )
        {
            result = e.getMessage();
        }
        catch( AmazonClientException e )
        {
            result = e.getMessage();
        }

        return result;
    }

    private long[] getRange( TaskInfo taskInfo )
    {
        long[] result = null;

        if ( taskInfo.partialSize < taskInfo.objectSize )
        {
            result = new long[2];
            Random random = new Random();
            result[0] = random.nextInt( taskInfo.objectSize - taskInfo.partialSize );
            result[1] = result[0] + taskInfo.partialSize - 1;
        }

        return result;
    }

    public String read( TaskInfo taskInfo, String objectId )
    {
        String result = null;
        try
        {
            long[] range = getRange( taskInfo );
            GetObjectRequest request = new GetObjectRequest( taskInfo.bucketName, objectId );

            if ( range != null )
            {
                request.setRange( range[0], range[1] );
            }

            S3Object object = taskInfo.s3.getObject( request );
            ObjectMetadata metadata = object.getObjectMetadata();

            long contentLength = metadata.getContentLength();

            if ( contentLength == this.data.length )
            {
                result = readContent( object );
            }
            else
            {
                result = "Unexpected object size.  Expected: " + this.data.length + ", actual: " + contentLength;
            }
        }
        catch ( AmazonServiceException e )
        {
            result = e.getMessage();
        }
        catch ( AmazonClientException e )
        {
            result = e.getMessage();
        }
        return result;
    }

    private String readContent( S3Object object )
    {
        String result = null;
        try
        {
            S3ObjectInputStream inputStream = object.getObjectContent();
            DataInputStream dataStream = new DataInputStream( inputStream );

            dataStream.readFully( this.data );
            dataStream.close();
        }
        catch( EOFException e )
        {
            result = "Unexpected end of file while reading data stream";
        }
        catch( IOException e )
        {
            result = "IOException while reading data stream";
        }

        return result;
    }

    public String verifyData( String bucketName, String objectId )
    {
        byte[] checkBytes = checksum( bucketName, objectId );

        if ( this.data.length < checkBytes.length )
        {
            return "Object data too short to contain integrity check";
        }

        ByteBuffer expected = ByteBuffer.wrap( checkBytes );

        ByteBuffer actual = ByteBuffer.wrap( this.data, 0, checkBytes.length );
        if ( !expected.equals( actual ) )
        {
            String expectedString = new String( checkBytes, 0, checkBytes.length, StandardCharsets.UTF_8 );
            return "Integrity match error at start.  Expected: '" + expectedString +"', actual: '" + new String( this.data, 0, checkBytes.length, StandardCharsets.UTF_8 );
        }

        int offset = this.data.length - checkBytes.length;
        actual = ByteBuffer.wrap( this.data, offset, checkBytes.length );
        if ( !expected.equals( actual ) )
        {
            String expectedString = new String( checkBytes, 0, checkBytes.length, StandardCharsets.UTF_8 );
            return "Integrity match error at end.  Expected: '" + expectedString +"', actual: '" + new String( this.data, offset, checkBytes.length, StandardCharsets.UTF_8 );
        }

        return null;
    }
}


