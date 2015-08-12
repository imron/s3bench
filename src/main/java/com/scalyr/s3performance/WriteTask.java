package com.scalyr.s3performance;

import com.scalyr.s3performance.DataObject;
import com.scalyr.s3performance.RandomObjectQueue;
import com.scalyr.s3performance.TaskInfo;
import com.scalyr.s3performance.Timer;

import java.util.Random;

class WriteTask
{
    private TaskInfo taskInfo;
    private DataObject dataObject;
    private RandomObjectQueue objectQueue;

    public WriteTask( TaskInfo taskInfo, RandomObjectQueue objectQueue )
    {
        this.taskInfo = taskInfo;
        this.objectQueue = objectQueue;

        this.dataObject = null;
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
        Random random = new Random();
        Timer timer = new Timer();
        String objectName = this.objectQueue.nextObject();
        while ( objectName != null )
        {
            prepare( random, objectName );

            timer.start();

            String error = this.dataObject.write( this.taskInfo, objectName );

            timer.stop();

            this.taskInfo.logResult( objectName, timer.elapsedMilliseconds(), error );

            objectName = this.objectQueue.nextObject();
        }
        
    }
}

