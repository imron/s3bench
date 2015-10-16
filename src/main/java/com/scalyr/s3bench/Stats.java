package com.scalyr.s3bench;

import com.scalyr.s3bench.Task;
import com.scalyr.s3bench.TaskInfo;
import com.scalyr.s3bench.Timer;

import java.util.List;

class Stats
{
    public int successfulOperations;
    public int errorCount;
    public long fastestOperation;
    public long slowestOperation;
    public long elapsedMilliseconds;
    public double currentThreadCpuUsedMilliseconds;
    public double processCpuUsedMilliseconds;
    public long bytesRead;

    public String operation;
    public int version;
    public String bucketName;
    public int threadCount;
    public long objectSize;
    public long partialSize;

    public Stats( List<Task> tasks, TaskInfo info, Timer timer )
    {

        this.operation = info.operation;
        this.version = info.version;
        this.bucketName = info.bucketName;
        this.threadCount = info.threadCount;
        this.objectSize = info.objectSize;
        this.partialSize = info.partialSize;

        this.successfulOperations = 0;
        this.errorCount = 0;
        this.fastestOperation = Long.MAX_VALUE;
        this.slowestOperation = Long.MIN_VALUE;
        this.elapsedMilliseconds = timer.elapsedMilliseconds();
        this.currentThreadCpuUsedMilliseconds = timer.currentThreadCpuUsedMilliseconds();
        this.processCpuUsedMilliseconds = timer.processCpuUsedMilliseconds();

        for( Task t : tasks )
        {
            this.successfulOperations += t.successfulOperations();
            this.errorCount += t.errorCount();
            this.fastestOperation = Math.min( t.fastestOperation(), this.fastestOperation );
            this.slowestOperation = Math.max( t.slowestOperation(), this.slowestOperation );
        }

        this.bytesRead = this.partialSize * this.successfulOperations;

    }

    public String key()
    {
        return this.operation + "-" + this.threadCount + "-" + this.partialSize + "-" + this.objectSize;
    }
}

