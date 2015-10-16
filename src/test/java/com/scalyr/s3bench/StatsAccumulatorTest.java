package com.scalyr.s3bench;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.scalyr.s3bench.Stats;
import com.scalyr.s3bench.StatsAccumulator;
import com.scalyr.s3bench.Task;
import com.scalyr.s3bench.TaskInfo;
import com.scalyr.s3bench.Timer;

class DummyTask extends Task
{
    public DummyTask( int successfulOperations )
    {
        super( null, null );
        this.successfulOperations = successfulOperations;
    }

    public void run()
    {
    }
}

/**
 * Unit test for simple App.
 */
public class StatsAccumulatorTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public StatsAccumulatorTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( StatsAccumulatorTest.class );
    }

    public void testBytesRead()
    {
        List<Task> tasks = new ArrayList<Task>();
        TaskInfo info = new TaskInfo( null, null, "test", "read", 16*1024*1024, 8, 1 );
        Timer timer = new Timer();
        timer.start();
        for ( int i = 0; i < 8; ++i )
        {
            Task task = new DummyTask( 40 );
            tasks.add( task );
        }
        timer.stop();
        Stats stats = new Stats( tasks, info, timer );

        long expected = 5L * 1024 * 1024 * 1024;

        assertEquals( expected, stats.bytesRead );
    }
}
