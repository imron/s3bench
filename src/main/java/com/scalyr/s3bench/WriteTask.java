package com.scalyr.s3bench;

import com.scalyr.s3bench.DataObject;
import com.scalyr.s3bench.RandomObjectQueue;
import com.scalyr.s3bench.Task;
import com.scalyr.s3bench.TaskInfo;
import com.scalyr.s3bench.Timer;

import java.util.Random;

class WriteTask extends Task
{
    public WriteTask( TaskInfo taskInfo, RandomObjectQueue objectQueue )
    {
        super( taskInfo, objectQueue );
    }

    public void prepare( Random random, String objectId )
    {
        if ( this.dataObject == null || this.dataObject.size() != taskInfo.objectSize )
        {
            this.dataObject = new DataObject( taskInfo.objectSize );
        }

        this.dataObject.fillBuffer( random, taskInfo.bucketName, objectId );
    }

    public void run()
    {
        reset();
        Random random = new Random();
        Timer timer = new Timer();
        String objectName = this.objectQueue.nextObject();
        while ( objectName != null )
        {
            prepare( random, objectName );

            timer.start();

            String error = this.dataObject.write( this.taskInfo, objectName );

            timer.stop();

            if ( error == null )
            {
                ++this.successfulOperations;
            }
            else
            {
                ++this.errorCount;
            }

            long elapsed = timer.elapsedMilliseconds();
            this.taskInfo.logResult( objectName, elapsed, error );
            operationComplete( elapsed );

            objectName = this.objectQueue.nextObject();
        }
        
    }
}

