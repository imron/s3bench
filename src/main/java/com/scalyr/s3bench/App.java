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
    private static final int DEFAULT_TASKS = 64;
    private static final int DEFAULT_TIMEOUT = 20000;
    private static final int MAX_CONNECTIONS = 64;
    private static final int MIN_BUCKET_NAME_LENGTH = 3;
    private static final int MAX_BUCKET_NAME_LENGTH = 63;


    private static final int KB_TO_BYTES = 1024;

    private static final int BUCKET_SIZE = 5*1024*1024*1024;
    private static final int DEFAULT_MAX_BUCKETS = 100;

    private static final int DEFAULT_LOOP_PAUSE = 5000;

    private static final String DEFAULT_BUCKET_PREFIX = "scalyr-s3-benchmark-";

    private int bucketSize;
    private String bucketPrefix;
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
        this.maxBuckets = DEFAULT_MAX_BUCKETS;
        this.loopPause = DEFAULT_LOOP_PAUSE;
        this.bucketSize = BUCKET_SIZE;
        this.bucketPrefix = DEFAULT_BUCKET_PREFIX;
        this.bucketList = new ArrayList<BucketInfo>();
        this.bucketList.ensureCapacity( this.maxBuckets );
        this.randomSelector = new Random( System.currentTimeMillis() );
    }
    private int randomObjectSizeInBytes()
    {
        int index = this.randomSelector.nextInt( OBJECT_SIZES_IN_KB.length );
        return OBJECT_SIZES_IN_KB[index] * KB_TO_BYTES;
    }

    private int objectSizeFromBucketName( String name )
    {
        int offset = 0;
        if ( name.startsWith( this.bucketPrefix ) )
        {
            offset = this.bucketPrefix.length();
        }

        int result = 0;
        int index = name.indexOf( "-", offset );
        if ( index > 0 )
        {
            String sizeString = name.substring( offset, index );
            try
            {
                result = Integer.parseInt( sizeString );
            }
            catch ( NumberFormatException e )
            {
                System.out.println( "Invalid object size in bucket name: " + name );
            }
        }
        return result * KB_TO_BYTES;
    }

    private int randomReadThreadCount()
    {
        int index = this.randomSelector.nextInt( READ_THREAD_COUNTS.length );
        return READ_THREAD_COUNTS[index];
    }

    private String uniqueBucketName( int objectSize )
    {
        return this.uniqueBucketName( objectSize, "" );
    }

    private String uniqueBucketName( int objectSize, String text ) throws RuntimeException
    {
        int sizeInKB = objectSize / KB_TO_BYTES;
        String result = this.bucketPrefix + Integer.toString( sizeInKB ) + "-" + text + UUID.randomUUID();

        if ( result.length() < MIN_BUCKET_NAME_LENGTH || result.length() > MAX_BUCKET_NAME_LENGTH )
        {
            throw new RuntimeException( "App.uniqueBucketName - '" + result + "' must be between 3 and 63 characters long.  Actual length is " + result.length() );
        }

        return result;
    }

    private void createBuckets( AmazonS3Client s3, int totalBuckets )
    {
        for ( int i = 0; i < totalBuckets; ++i )
        {
            int objectSize = randomObjectSizeInBytes();
            String bucketName = uniqueBucketName( objectSize, "" + i + "-" );
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

    private void deleteBucket( AmazonS3Client s3, String bucketName ) throws RuntimeException
    {
        if ( !bucketName.startsWith( this.bucketPrefix ) )
        {
            throw new RuntimeException( "Trying to delete a bucket we probably shouldn't: " + bucketName );
        }

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

    private List<Bucket> listBuckets( AmazonS3Client s3 )
    {
        List<Bucket> list = s3.listBuckets();

        ArrayList<Bucket> result = new ArrayList<Bucket>( list.size() );
        for ( Bucket b : list )
        {
            if ( b.getName().startsWith( this.bucketPrefix ) )
            {
                result.add( b );
            }
        }

        return result;
    }

    private void deleteAllBuckets( AmazonS3Client s3 )
    {
        List<Bucket> list = listBuckets( s3 );
        for ( Bucket b : list )
        {
            String name = b.getName();
            if ( name.startsWith( this.bucketPrefix ) )
            {
                deleteBucket( s3, b.getName() );
            }
        }
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

    private Bucket getRandomBucket( List<Bucket> bucketList )
    {
        int index = this.randomSelector.nextInt( bucketList.size() );
        return bucketList.get( index );
    }

    private void prepareRead( List<Bucket> bucketList, ArrayList<Task> tasks, TaskInfo info )
    {
        tasks.clear();

        if ( bucketList.isEmpty() )
        {
            info.logger.error( "App.prepareRead - Attempting a read operation on an empty list" );
            return;
        }

        Bucket bucket = getRandomBucket( bucketList );

        int objectSize = objectSizeFromBucketName( bucket.getName() );

        if ( objectSize != 0 )
        {
            info.bucketName = bucket.getName();
            info.operation = "read";
            info.objectSize = objectSize;

            int totalObjects = this.bucketSize / objectSize;
            RandomIdBuffer rid = new RandomIdBuffer( totalObjects );
            rid.setBlockCount( info.threadCount );

            for ( int i = 0; i < info.threadCount; ++i )
            {
                RandomObjectQueue roq = new RandomObjectQueue( rid.getBlock( i ) );
                ReadTask readTask = new ReadTask( info, roq );
                tasks.add( readTask );
            }
        }
    }

    private void prepareWrite( List<Bucket> bucketList, ArrayList<Task> tasks, TaskInfo info )
    {
        tasks.clear();

        int objectSize = randomObjectSizeInBytes();
        String bucketName = uniqueBucketName( objectSize );
        Bucket bucket = info.s3.createBucket( bucketName );

        if ( bucket == null )
        {
            info.logger.error( "App.prepareWrite - Error creating new bucket '" + bucketName + "'." );
            return;
        }

        bucketList.add( bucket );

        info.bucketName = bucketName;
        info.operation = "write";
        info.objectSize = objectSize;

        int totalObjects = this.bucketSize / objectSize;

        RandomIdBuffer rid = new RandomIdBuffer( totalObjects );
        rid.setBlockCount( info.threadCount );

        for ( int i = 0; i < info.threadCount; ++i )
        {
            RandomObjectQueue roq = new RandomObjectQueue( rid.getBlock( i ) );
            WriteTask writeTask = new WriteTask( info, roq );
            tasks.add( writeTask );
        }
    }

    private void logSummary( List<Task> tasks, TaskInfo info, Timer timer )
    {
        int successfulOperations = 0;
        int errorCount = 0;
        for( Task t : tasks )
        {
            successfulOperations += t.successfulOperations();
            errorCount += t.errorCount();
        }

        info.logger.info( "op=%sSummary threads=%d size=%d bucket=%s successfulOperations=%d errorCount=%d elapsedTimeMs=%d currentThreadCpuUsedMs=%f processCpuUsedMs=%f",
                         info.operation, info.threadCount, info.objectSize, info.bucketName, successfulOperations, errorCount, timer.elapsedMilliseconds(), timer.currentThreadCpuUsedMilliseconds(), timer.processCpuUsedMilliseconds() );

    }

    private void performWork( ArrayList<Task> tasks, TaskInfo info, Timer timer ) throws InterruptedException
    {
        ArrayList<Thread> threads = new ArrayList<Thread>( info.threadCount );
        for ( Task task : tasks )
        {
            Thread t = new Thread( task );
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
    }


    private void run()
    {
        try
        {
            AmazonS3Client s3 = createS3Client();

            Logger logger = LogManager.getFormatterLogger( App.class );
            TaskInfo info = new TaskInfo( logger, s3, null, null, 0, 0 );

            Timer timer = new Timer();

            List<Bucket> list = listBuckets( s3 );

            ArrayList<Task> tasks = new ArrayList<Task>( DEFAULT_TASKS );
            //while ( true )
            for ( int i = 0; i < 10000; ++i )
            {
                info.threadCount = randomReadThreadCount();

                Operation op = chooseOperation( list );
                switch ( op )
                {
                    case READ:
                        prepareRead( list, tasks, info );
                        break;
                    case WRITE:
                        prepareWrite( list, tasks, info );
                        break;
                }

                performWork( tasks, info, timer );

                logSummary( tasks, info, timer );

                Thread.sleep( this.loopPause );
            }
        }
        catch ( InterruptedException e )
        {
            System.out.println( "Program was interrupted.  Exiting." );
        }
    }

    private Operation chooseOperation( List<Bucket> bucketList )
    {
        Operation result = Operation.READ;
        if ( bucketList.size() < this.maxBuckets )
        {
            result = Operation.WRITE;
        }
        return result;
    }
}

