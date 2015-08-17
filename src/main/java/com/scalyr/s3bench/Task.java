package com.scalyr.s3bench;

abstract class Task implements Runnable
{
    protected TaskInfo taskInfo;
    protected RandomObjectQueue objectQueue;
    protected DataObject dataObject;
    protected int successfulOperations;
    protected int errorCount;

    private long fastestOperation;
    private long slowestOperation;

    public Task( TaskInfo taskInfo, RandomObjectQueue objectQueue )
    {
        this.taskInfo = taskInfo;
        this.objectQueue = objectQueue;
        this.dataObject = null;
        reset();
    }

    protected void reset()
    {
        this.successfulOperations = 0;
        this.errorCount = 0;
        this.fastestOperation = Long.MAX_VALUE;
        this.slowestOperation = Long.MIN_VALUE;
    }

    protected void operationComplete( long millis )
    {
        this.fastestOperation = Math.min( millis, this.fastestOperation );
        this.slowestOperation = Math.max( millis, this.slowestOperation );
    }

    public long fastestOperation()
    {
        return this.fastestOperation;
    }

    public long slowestOperation()
    {
        return this.slowestOperation;
    }

    public int successfulOperations()
    {
        return this.successfulOperations;
    }

    public int errorCount()
    {
        return this.errorCount;
    }
}

