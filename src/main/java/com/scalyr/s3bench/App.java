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

import com.scalyr.api.logs.*;

import com.scalyr.s3bench.BucketInfo;
import com.scalyr.s3bench.RandomIdBuffer;
import com.scalyr.s3bench.RandomObjectQueue;
import com.scalyr.s3bench.Stats;
import com.scalyr.s3bench.StatsAccumulator;
import com.scalyr.s3bench.Timer;

import java.io.FileInputStream;
import java.nio.IntBuffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.commons.lang3.text.WordUtils;


public class App 
{
    enum Operation
    {
        READ,
        WRITE
    }

    private static final int VERSION = 4;

    private static final int SCALYR_BUFFER_RAM = 4*1024*1024;

    private static final int MAX_BUCKET_TRIES = 100;
    private static final int ONE_MB = 1024*1024;
    private static final int FOUR_MB = 4*ONE_MB;
    private static final int SIXTEEN_MB = 16*ONE_MB;

    private static final String DEFAULT_PROPERTY_FILE = "s3bench.properties";

    private static final int PARTIAL_READ_SIZE = 256*1024;
    private static final int[] OBJECT_SIZES_IN_KB = { 256, 1024, 4096, 16384 };


    private static final int[] OBJECT_SIZES = { ONE_MB, FOUR_MB, SIXTEEN_MB };
    private static final int[] READ_THREAD_COUNTS = { 1, 2, 4, 8, 16, 32, 64, 128 };
    private static final int[] READ_THREAD_COUNTS_1MB = { 16, 32, 64 };
    private static final int[] READ_THREAD_COUNTS_4MB = { 4, 8, 16, 32 };
    private static final int[] READ_THREAD_COUNTS_16MB = { 4, 8, 16, 32 };
    private static final int[] WRITE_THREAD_COUNTS = { 1, 2, 4, 8 };

    private static final int DEFAULT_TASKS = 64;

    private static final int LARGE_THREAD_THRESHOLD = 1024*1024;

    private static final int MIN_BUCKET_NAME_LENGTH = 3;
    private static final int MAX_BUCKET_NAME_LENGTH = 63;

    private static final int KB_TO_BYTES = 1024;
    private static final double MILLIS_TO_SECONDS = 1e-3;
    private static final int MB_TO_BYTES = 1024*1024;

    private static final String BUCKET_SIZE_KEY = "bucketSize";
    private static final long DEFAULT_BUCKET_SIZE = 5L*1024*1024*1024;

    private static final String MAX_BUCKETS_KEY = "maxBuckets";
    private static final int DEFAULT_MAX_BUCKETS = 400;

    private static final String LOOP_DELAY_KEY = "loopDelay";
    private static final int DEFAULT_LOOP_DELAY = 5000;

    private static final String LOOP_ITERATIONS_KEY = "loopIterations";
    private static final int DEFAULT_LOOP_ITERATIONS = 1000;

    private static final String FREE_MEMORY_THRESHOLD_KEY = "freeMemoryThreshold";
    private static final long DEFAULT_FREE_MEMORY_THRESHOLD = 5*1024*1024*1024;

    private static final String SERVER_HOST_KEY = "serverHost";
    private static final String DEFAULT_SERVER_HOST = null;

    private static final String BUCKET_PREFIX_KEY = "bucketPrefix";
    private static final String DEFAULT_BUCKET_PREFIX = "scalyr-s3-benchmark-";

    private static final String AWS_ENDPOINT_KEY = "aws.endpoint";
    private static final String DEFAULT_AWS_ENDPOINT = "s3-external-1.amazonaws.com";

    private static final String AWS_MAX_CONNECTIONS_KEY = "aws.maxConnections";
    private static final int DEFAULT_AWS_MAX_CONNECTIONS = 64;

    private static final String AWS_CONNECTION_TIMEOUT_KEY = "aws.connectionTimeout";
    private static final int DEFAULT_AWS_CONNECTION_TIMEOUT = 20000;

    private static final String AWS_SOCKET_TIMEOUT_KEY = "aws.socketTimeout";
    private static final int DEFAULT_AWS_SOCKET_TIMEOUT = 20000;


    private long bucketSize;
    private String bucketPrefix;
    private String serverHost;
    private int maxBuckets;
    private int loopDelay;
    private int loopIterations;
    private long freeMemoryThreshold;

    private String awsEndpoint;
    private int awsMaxConnections;
    private int awsConnectionTimeout;
    private int awsSocketTimeout;

    private ArrayList<BucketInfo> bucketList;
    private Random randomSelector;

    private HashMap<String, StatsAccumulator> accumulators;

    public static void main( String[] args ) throws Exception
    {
        String propertyFile = DEFAULT_PROPERTY_FILE;
        if ( args.length == 1 )
        {
            propertyFile = args[0];
        }
        else if ( args.length > 1 )
        {
            System.out.println( "Usage: App [property file]" );
        }

        App app = new App( propertyFile );
        try
        {
            app.run();
        }
        catch ( Exception e )
        {
            System.out.println( "Unexpected exception - application will exit.  " + e.getMessage() );
            throw e;
        }
    }

    public void initScalyr()
    {
        EventAttributes attributes = null;
        if ( this.serverHost != null )
        {
            attributes = new EventAttributes( "serverHost", this.serverHost );
        }
        Events.init("0Le5BQxEFBZ_od5cA0biuP2lWhIeXIZcNGn4hvB5ftak-", SCALYR_BUFFER_RAM, null, attributes );
        StatReporter.registerAll();
    }

    public App( String properties )
    {
        this.maxBuckets = DEFAULT_MAX_BUCKETS;
        this.loopDelay = DEFAULT_LOOP_DELAY;
        this.loopIterations = DEFAULT_LOOP_ITERATIONS;
        this.bucketSize = DEFAULT_BUCKET_SIZE;
        this.bucketPrefix = DEFAULT_BUCKET_PREFIX;
        this.freeMemoryThreshold = DEFAULT_FREE_MEMORY_THRESHOLD;

        this.serverHost = DEFAULT_SERVER_HOST;

        this.awsEndpoint = DEFAULT_AWS_ENDPOINT;
        this.awsMaxConnections = DEFAULT_AWS_MAX_CONNECTIONS;
        this.awsConnectionTimeout = DEFAULT_AWS_CONNECTION_TIMEOUT;
        this.awsSocketTimeout = DEFAULT_AWS_SOCKET_TIMEOUT;

        loadProperties( properties );

        this.bucketList = new ArrayList<BucketInfo>();
        this.bucketList.ensureCapacity( this.maxBuckets );
        this.randomSelector = new Random( System.currentTimeMillis() );

        int totalReadKeys = READ_THREAD_COUNTS.length * OBJECT_SIZES_IN_KB.length;
        int totalWriteKeys = WRITE_THREAD_COUNTS.length * OBJECT_SIZES_IN_KB.length;
        this.accumulators = new HashMap<String, StatsAccumulator>( (totalReadKeys + totalWriteKeys) * 2 );
    }

    int loadInt( Properties properties, String key, int defaultValue )
    {
        String value = properties.getProperty( key, Integer.toString( defaultValue ) );

        int result = defaultValue;

        try
        {
            result = Integer.parseInt( value );
        }
        catch ( NumberFormatException e )
        {
            System.out.println( key + ": " + value + " is not a valid int." );
        }

        return result;

    }

    long loadLong( Properties properties, String key, long defaultValue )
    {
        String value = properties.getProperty( key, Long.toString( defaultValue ) );

        long result = defaultValue;

        try
        {
            result = Long.parseLong( value );
        }
        catch ( NumberFormatException e )
        {
            System.out.println( key + ": " + value + " is not a valid long." );
        }

        return result;

    }

    void loadProperties( String filename )
    {
        FileInputStream inputStream = null;

        try
        {
            inputStream = new FileInputStream( filename );
        }
        catch( Exception e )
        {
            //ignore
        }

        if ( inputStream != null )
        {

            Properties properties = new Properties();

            try
            {
                properties.load( new FileInputStream( filename ) );

                this.maxBuckets = loadInt( properties, MAX_BUCKETS_KEY, DEFAULT_MAX_BUCKETS );
                this.loopDelay = loadInt( properties, LOOP_DELAY_KEY, DEFAULT_LOOP_DELAY );
                this.loopIterations = loadInt( properties, LOOP_ITERATIONS_KEY, DEFAULT_LOOP_ITERATIONS );
                this.bucketSize = loadLong( properties, BUCKET_SIZE_KEY, DEFAULT_BUCKET_SIZE );
                this.freeMemoryThreshold = loadLong( properties, FREE_MEMORY_THRESHOLD_KEY, DEFAULT_FREE_MEMORY_THRESHOLD );

                this.bucketPrefix = properties.getProperty( BUCKET_PREFIX_KEY, DEFAULT_BUCKET_PREFIX );
                this.serverHost = properties.getProperty( SERVER_HOST_KEY, DEFAULT_SERVER_HOST );

                this.awsEndpoint = properties.getProperty( AWS_ENDPOINT_KEY, DEFAULT_AWS_ENDPOINT );
                this.awsMaxConnections = loadInt( properties, AWS_MAX_CONNECTIONS_KEY, DEFAULT_AWS_MAX_CONNECTIONS );
                this.awsConnectionTimeout = loadInt( properties, AWS_CONNECTION_TIMEOUT_KEY, DEFAULT_AWS_CONNECTION_TIMEOUT );
                this.awsSocketTimeout = loadInt( properties, AWS_SOCKET_TIMEOUT_KEY, DEFAULT_AWS_SOCKET_TIMEOUT );

            }
            catch( Exception e )
            {
                System.out.println( "Error loading property file: '" + filename + "'." );
            }
        }
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
                System.out.println( "Invalid object size at offset " + offset + " in bucket name: " + name );
            }
        }
        return result * KB_TO_BYTES;
    }

    private int randomReadThreadCount( int objectSize )
    {
        int[] threadCounts = READ_THREAD_COUNTS;

        switch ( objectSize )
        {
            case ONE_MB:
                threadCounts = READ_THREAD_COUNTS_1MB;
                break;
            case FOUR_MB:
                threadCounts = READ_THREAD_COUNTS_4MB;
                break;
            case SIXTEEN_MB:
                threadCounts = READ_THREAD_COUNTS_16MB;
                break;
        }
        int maxOffset = threadCounts.length;
        int index = this.randomSelector.nextInt( maxOffset );
        return threadCounts[index];
    }

    private int randomWriteThreadCount()
    {
        int index = this.randomSelector.nextInt( WRITE_THREAD_COUNTS.length );
        return WRITE_THREAD_COUNTS[index];
    }

    private boolean randomBoolean( float weight )
    {
        return this.randomSelector.nextFloat() < weight;
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

        configuration.setConnectionTimeout( this.awsConnectionTimeout );
        configuration.setSocketTimeout( this.awsSocketTimeout );
        configuration.setMaxConnections( this.awsMaxConnections );

        AmazonS3Client s3 = new AmazonS3Client( configuration );
        Region usEast1 = Region.getRegion( Regions.US_EAST_1 );
        s3.setRegion( usEast1 );
        s3.setEndpoint( this.awsEndpoint );
        return s3;
    }

    private Bucket getRandomBucketOfSize( List<Bucket> bucketList, int size )
    {
        Bucket bucket = null;
        int MAX_ITERATIONS = 100;
        int count = 0;
        while ( bucket == null && count < MAX_ITERATIONS )
        {
            bucket = getRandomBucket( bucketList );
            int objectSize = objectSizeFromBucketName( bucket.getName() );
            if ( objectSize != size )
            {
                bucket = null;
            }
            ++count;
        }
        return bucket;
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

        Bucket bucket = null;

        int count = 0;
        int objectSize = 0;
        while ( bucket == null && count < MAX_BUCKET_TRIES )
        {
            Bucket nextBucket = getRandomBucket( bucketList );
            objectSize = objectSizeFromBucketName( nextBucket.getName() );
            for ( int size : OBJECT_SIZES )
            {
                if ( size == objectSize )
                {
                    bucket = nextBucket;
                    break;
                }
            }
            ++count;
        }

        if ( bucket == null )
        {
            info.logger.error( "App.prepareRead - Failed to get bucket" );
            return;
        }

        if ( objectSize != 0 )
        {
            info.threadCount = randomReadThreadCount( objectSize );
            info.bucketName = bucket.getName();
            info.objectSize = objectSize;
            info.partialSize = objectSize;
            info.operation = "read";

            boolean partialRead = false;
            if ( objectSize == ONE_MB )
            {
                // Commenting out the following line to ensure that
                // partial reads are always false
                //partialRead = randomBoolean( 0.5f );
            }

            int totalObjects = (int)(this.bucketSize / objectSize);
            RandomIdBuffer rid = new RandomIdBuffer( totalObjects );
            rid.setBlockCount( info.threadCount );

            if ( partialRead )
            {
                info.operation = "partialRead";
                info.partialSize = PARTIAL_READ_SIZE;
            }

            for ( int i = 0; i < info.threadCount; ++i )
            {
                RandomObjectQueue roq = new RandomObjectQueue( rid.getBlock( i ) );
                ReadTask task = new ReadTask( info, roq );
                tasks.add( task );
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

        info.threadCount = randomWriteThreadCount();
        info.bucketName = bucketName;
        info.operation = "write";
        info.objectSize = objectSize;
        info.partialSize = objectSize;

        int totalObjects = (int)(this.bucketSize / objectSize);

        RandomIdBuffer rid = new RandomIdBuffer( totalObjects );
        rid.setBlockCount( info.threadCount );

        for ( int i = 0; i < info.threadCount; ++i )
        {
            RandomObjectQueue roq = new RandomObjectQueue( rid.getBlock( i ) );
            WriteTask writeTask = new WriteTask( info, roq );
            tasks.add( writeTask );
        }
    }

    private double bytesAndMillisecondsToMbPerSecond( long bytes, long milliseconds )
    {
        double elapsedSeconds = milliseconds * MILLIS_TO_SECONDS;
        double mbRead = (double)bytes / MB_TO_BYTES;
        return mbRead / elapsedSeconds;
    }

    private void logSummary( Logger logger, Stats stats, StatsAccumulator accumulator )
    {
        double mbPerSecond = bytesAndMillisecondsToMbPerSecond( stats.bytesRead, stats.elapsedMilliseconds );

        logger.info( "op=%sSummary version=%d threads=%d size=%d objectSize=%d mbPerSecond=%f bucket=%s successfulOperations=%d errorCount=%d fastestOperation=%d slowestOperation=%d elapsedTimeMs=%d currentThreadCpuUsedMs=%f processCpuUsedMs=%f",
                         stats.operation, stats.version, stats.threadCount, stats.partialSize, stats.objectSize, mbPerSecond, stats.bucketName, stats.successfulOperations, stats.errorCount, stats.fastestOperation, stats.slowestOperation, stats.elapsedMilliseconds, stats.currentThreadCpuUsedMilliseconds, stats.processCpuUsedMilliseconds );

        mbPerSecond = bytesAndMillisecondsToMbPerSecond( accumulator.bytesRead, accumulator.elapsedMilliseconds );

        logger.info( "op=cumulative%sSummary version=%d threads=%d size=%d objectSize=%d mbPerSecond=%f successfulOperations=%d errorCount=%d fastestOperation=%d slowestOperation=%d elapsedTimeMs=%d currentThreadCpuUsedMs=%f processCpuUsedMs=%f",
                         WordUtils.capitalize( accumulator.operation ), accumulator.version, accumulator.threadCount, accumulator.partialSize, accumulator.objectSize, mbPerSecond, accumulator.successfulOperations, accumulator.errorCount, accumulator.fastestOperation, accumulator.slowestOperation, accumulator.elapsedMilliseconds, accumulator.currentThreadCpuUsedMilliseconds, accumulator.processCpuUsedMilliseconds );
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

    private long collectGarbage( Logger logger, TaskInfo info )
    {
        long elapsed = 0;
        long freeMemory = Runtime.getRuntime().freeMemory();
        if ( freeMemory < this.freeMemoryThreshold )
        {
            Timer timer = new Timer();
            timer.start();
            System.gc();
            timer.stop();

            elapsed = timer.elapsedMilliseconds();

            logger.info( "op=gc version=%d freeMemoryBefore=%d freeMemoryAfter=%d elapsedTimeMs=%d", info.version, freeMemory, Runtime.getRuntime().freeMemory(), elapsed );
        }

        return elapsed;

    }

    private StatsAccumulator getAccumulator( String key )
    {
        StatsAccumulator result = this.accumulators.get( key );
        if ( result == null )
        {
            result = new StatsAccumulator();
            this.accumulators.put( key, result );
        }
        return result;
    }


    private void run()
    {
        try
        {
            initScalyr();

            AmazonS3Client s3 = createS3Client();

            Logger logger = LogManager.getFormatterLogger( App.class );
            TaskInfo info = new TaskInfo( logger, s3, null, null, 0, 0, VERSION );

            StatsAccumulator dummyAccumulator = new StatsAccumulator();
            Timer timer = new Timer();

            List<Bucket> list = listBuckets( s3 );

            ArrayList<Task> tasks = new ArrayList<Task>( DEFAULT_TASKS );
            while ( true )
            //for ( int i = 0; i < this.loopIterations; ++i )
            {
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

                Stats stats = new Stats( tasks, info, timer );
                StatsAccumulator accumulator = getAccumulator( stats.key() );
                accumulator.accumulate( stats );

                logSummary( info.logger, stats, accumulator );

                tasks.clear();

                long gcDelay = collectGarbage( logger, info );
                long delay = Math.max( 0, this.loopDelay - gcDelay );
                Thread.sleep( delay );
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

