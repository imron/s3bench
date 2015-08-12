package com.scalyr.s3bench;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.scalyr.s3bench.BucketInfo;
import com.scalyr.s3bench.RandomIdBuffer;
import com.scalyr.s3bench.RandomObjectQueue;
import com.scalyr.s3bench.Timer;

import java.nio.IntBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



public class App 
{
    enum Operation
    {
        READ,
        WRITE
    }

    private static final int[] OBJECT_SIZES_IN_KB = { 256, 1024, 4096, 16384 };
    private static final int[] READ_THREAD_COUNTS = { 1, 2, 4, 8, 16, 32, 64 };
    private static final int DEFAULT_TIMEOUT = 20000;
    private static final int MAX_CONNECTIONS = 64;

    private static final int BUCKET_SIZE = 64*1024;

    private int bucketSize;
    private int maxBuckets;
    private int loopPause;
    private ArrayList<BucketInfo> bucketList;
    private Random randomSelector;

    public static void main( String[] args )
    {
        App app = new App();
        app.run();
    }

    public App()
    {
        this.maxBuckets = 10;
        this.loopPause = 5000;
        this.bucketSize = BUCKET_SIZE;
        this.bucketList = new ArrayList<BucketInfo>();
        this.bucketList.ensureCapacity( this.maxBuckets );
        this.randomSelector = new Random( System.currentTimeMillis() );
    }

    private int randomObjectSizeInBytes()
    {
        int index = this.randomSelector.nextInt( OBJECT_SIZES_IN_KB.length );
        return OBJECT_SIZES_IN_KB[index];
    }

    private int randomReadThreadCount()
    {
        int index = this.randomSelector.nextInt( READ_THREAD_COUNTS.length );
        return READ_THREAD_COUNTS[index];
    }

    private String uniqueBucketName( int objectSize, String text )
    {
        return Integer.toString( objectSize ) + "-" + text + "-" + UUID.randomUUID();
    }

    private void createBuckets( AmazonS3Client s3, int totalBuckets )
    {
        for ( int i = 0; i < totalBuckets; ++i )
        {
            int objectSize = randomObjectSizeInBytes();
            String bucketName = uniqueBucketName( objectSize, "test-" + i );
            createBucket( s3, bucketName, objectSize );
        }
    }

    private void createBucket( AmazonS3Client s3, String bucketName, int objectSize )
    {
        s3.createBucket( bucketName );

        int totalObjects = this.bucketSize / objectSize;

        RandomIdBuffer rid = new RandomIdBuffer( totalObjects );
        IntBuffer ints = rid.getBlock( 0 );

        RandomObjectQueue roq = new RandomObjectQueue( ints );

        Logger logger = LogManager.getFormatterLogger( App.class );

        TaskInfo info = new TaskInfo( logger, s3, bucketName, "write", objectSize, 1 );

        WriteTask writeTask = new WriteTask( info, roq );

        writeTask.run();
    }

    private int objectSizeFromBucketName( String name )
    {
        int result = 0;
        int index = name.indexOf( "-" );
        if ( index > 0 )
        {
            String sizeString = name.substring( 0, index );
            try
            {
                result = Integer.parseInt( sizeString );
            }
            catch ( NumberFormatException e )
            {
                System.out.println( "Invalid object size in bucket name: " + name );
            }
        }
        return result;
    }

    private void deleteBucket( AmazonS3Client s3, String bucketName )
    {
        ArrayList<KeyVersion> keys = new ArrayList<KeyVersion>();
        ObjectListing objectListing;

        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
            .withBucketName( bucketName );

        do
        {
            objectListing = s3.listObjects( listObjectsRequest );
            List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();

            if ( !objectSummaries.isEmpty() )
            {
                keys.clear();
                keys.ensureCapacity( objectSummaries.size() );

                for ( S3ObjectSummary objectSummary : objectSummaries )
                {
                    keys.add( new KeyVersion( objectSummary.getKey() ) );
                }

                listObjectsRequest.setMarker( objectListing.getNextMarker() );

                DeleteObjectsRequest multipleDeleteRequest = new DeleteObjectsRequest( bucketName ).withKeys( keys );
                s3.deleteObjects( multipleDeleteRequest );
            }

        } while ( objectListing.isTruncated() );

        s3.deleteBucket( bucketName );

    }

    private void deleteAllBuckets( AmazonS3Client s3 )
    {
        List<Bucket> list = s3.listBuckets();
        for ( Bucket b : list )
        {
            deleteBucket( s3, b.getName() );
        }
    }

    private void loadBucketList( AmazonS3Client s3 )
    {
        List<Bucket> list = s3.listBuckets();

        if ( list.size() < this.maxBuckets )
        {
            //createBuckets( s3, this.maxBuckets - list.size() );
            list = s3.listBuckets();
        }

        System.out.println( "Bucket names:\n----------" );
        for ( Bucket b : list )
        {
            System.out.println( "\tBucket: " + b.getName() );
        }


        test( s3 );

    }

    private void test( AmazonS3Client s3 )
    {
        String bucketName = "imron-test";
        s3.createBucket( bucketName );
        RandomIdBuffer rid = new RandomIdBuffer( 10 );

        IntBuffer ints = rid.getBlock( 0 );

        RandomObjectQueue roq = new RandomObjectQueue( ints );

        Logger logger = LogManager.getFormatterLogger( "Test" );

        TaskInfo info = new TaskInfo( logger, s3, bucketName, "write", 256*1024, 1 );

        WriteTask writeTask = new WriteTask( info, roq );

        writeTask.run();

        roq = new RandomObjectQueue( rid.getBlock( 0 ) );
        info.operation = "read";

        info.logger.error( "about to create read task" );
        ReadTask readTask = new ReadTask( info, roq );

        info.logger.error( "about to run read task" );
        readTask.run();

        deleteBucket( s3, bucketName );
    }

    private AmazonS3Client createS3Client()
    {
        ClientConfiguration configuration = new ClientConfiguration();

        configuration.setConnectionTimeout( DEFAULT_TIMEOUT );
        configuration.setSocketTimeout( DEFAULT_TIMEOUT );
        configuration.setMaxConnections( MAX_CONNECTIONS );

        AmazonS3Client s3 = new AmazonS3Client( configuration );
        Region usEast1 = Region.getRegion( Regions.US_EAST_1 );
        s3.setRegion( usEast1 );
        return s3;
    }


    private void run()
    {
        try
        {
            AmazonS3Client s3 = createS3Client();
            //deleteAllBuckets( s3 );
            //createBuckets( s3, this.maxBuckets );
            Logger logger = LogManager.getFormatterLogger( App.class );
            TaskInfo info = new TaskInfo( logger, s3, null, null, 0, 0 );

            Timer timer = new Timer();

            List<Bucket> list = s3.listBuckets();
            //while ( true )
            for ( int i = 0; i < 10; ++i )
            {
                int index = this.randomSelector.nextInt( list.size() );
                Bucket bucket = list.get( index );

                int objectSize = objectSizeFromBucketName( bucket.getName() );
                if ( objectSize != 0 )
                {
                    int threadCount = randomReadThreadCount();

                    info.bucketName = bucket.getName();
                    info.operation = "read";
                    info.objectSize = objectSize;
                    info.threadCount = threadCount;

                    int totalObjects = this.bucketSize / objectSize;
                    RandomIdBuffer rid = new RandomIdBuffer( totalObjects );
                    rid.setBlockCount( threadCount );

                    ArrayList<ReadTask> tasks = new ArrayList<ReadTask>( threadCount );
                    for ( int j = 0; j < threadCount; ++j )
                    {
                        RandomObjectQueue roq = new RandomObjectQueue( rid.getBlock( j ) );
                        ReadTask readTask = new ReadTask( info, roq );
                        tasks.add( readTask );
                    }

                    ArrayList<Thread> threads = new ArrayList<Thread>( threadCount );
                    for ( ReadTask r : tasks )
                    {
                        Thread t = new Thread( r );
                        threads.add( t );
                    }

                    timer.start();
                    for ( Thread t : threads )
                    {
                        t.start();
                    }

                    for ( Thread t : threads )
                    {
                        t.join();
                    }

                    timer.stop();
                    logger.info( "op=readSummary threads=%d size=%d bucket=%s elapsedTimeMs=%d cpuUsedMs=%d",
                                 threadCount, objectSize, info.bucketName, timer.elapsedMilliseconds(), timer.cpuUsedMilliseconds() );
                }


                

                Thread.sleep( this.loopPause );
            }
        }
        catch ( InterruptedException e )
        {
            System.out.println( "Program was interrupted.  Exiting." );
        }
    }

    private Operation chooseOperation()
    {
        Operation result = Operation.READ;
        if ( this.bucketList.size() < this.maxBuckets )
        {
            result = Operation.WRITE;
        }
        return result;
    }

    private void prepareOperation( Operation op )
    {
        switch ( op )
        {
            case READ:
                prepareRead();
                break;
            case WRITE:
                prepareWrite();
                break;
        }
    }

    private BucketInfo prepareWrite()
    {
        BucketInfo bucket = null;
        if ( this.bucketList.size() < this.maxBuckets )
        {
            bucket = new BucketInfo();
            this.bucketList.add( bucket );
        }
        else
        {
            int index = this.randomSelector.nextInt( this.bucketList.size() );

            BucketInfo oldBucket = this.bucketList.get( index );

            deleteBucket( oldBucket );

            bucket = new BucketInfo();
            this.bucketList.set( index, bucket );
        }

        return bucket;
    }

    private void deleteBucket( BucketInfo bucket )
    {
    }


    private void prepareRead()
    {
    }

    private void performWork()
    {
        System.out.println( "Working" );
    }
}

