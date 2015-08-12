package com.scalyr.s3performance;

import com.scalyr.s3performance.DataObject;
import com.scalyr.s3performance.RandomObjectQueue;
import com.scalyr.s3performance.TaskInfo;
import com.scalyr.s3performance.Timer;

import java.io.DataInputStream;

import org.apache.logging.log4j.Logger;

class ReadTask implements Runnable
{
    private TaskInfo taskInfo;
    private RandomObjectQueue objectQueue;
    private DataObject dataObject;

    public ReadTask( TaskInfo taskInfo, RandomObjectQueue objectQueue )
    {
        this.taskInfo = taskInfo;
        this.objectQueue = objectQueue;
        this.dataObject = null;
    }

    public void prepare()
    {
        if ( this.dataObject == null || this.dataObject.size() != taskInfo.objectSize )
        {
            this.dataObject = new DataObject( taskInfo.objectSize );
        }
        else
        {
            this.dataObject.clear();
        }
    }

    public void run()
    {
        Timer timer = new Timer();
        String objectName = this.objectQueue.nextObject();
        while ( objectName != null )
        {
            prepare();

            timer.start();

            String error = this.dataObject.read( this.taskInfo, objectName );

            timer.stop();

            if ( error == null )
            {
                error = this.dataObject.verifyData( this.taskInfo.bucketName, objectName );
            }

            this.taskInfo.logResult( objectName, timer.elapsedMilliseconds(), error );

            objectName = this.objectQueue.nextObject();

        }
    }
}


