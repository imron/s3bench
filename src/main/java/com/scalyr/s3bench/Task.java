package com.scalyr.s3bench;

abstract class Task implements Runnable
{
    protected TaskInfo taskInfo;
    protected RandomObjectQueue objectQueue;
    protected DataObject dataObject;
    protected int successfulOperations;
    protected int errorCount;

    public Task( TaskInfo taskInfo, RandomObjectQueue objectQueue )
    {
        this.taskInfo = taskInfo;
        this.objectQueue = objectQueue;
        this.dataObject = null;
        this.successfulOperations = 0;
        this.errorCount = 0;
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

