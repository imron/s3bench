package com.scalyr.s3bench;

import com.scalyr.s3bench.Stats;

class StatsAccumulator
{
    public int successfulOperations;
    public int errorCount;
    public long fastestOperation;
    public long slowestOperation;
    public long elapsedMilliseconds;
    public double currentThreadCpuUsedMilliseconds;
    public double processCpuUsedMilliseconds;
    public long bytesRead;

    public String key;
    public String operation;
    public int version;
    public int threadCount;
    public int objectSize;

    public StatsAccumulator()
    {
        this.successfulOperations = 0;
        this.errorCount = 0;
        this.fastestOperation = Long.MAX_VALUE;
        this.slowestOperation = Long.MIN_VALUE;
        this.elapsedMilliseconds = 0;
        this.currentThreadCpuUsedMilliseconds = 0;
        this.processCpuUsedMilliseconds = 0;
        this.bytesRead = 0;
        this.operation = "invalid";
        this.version = 0;
        this.threadCount = 0;
        this.objectSize = 0;
        this.key = null;
    }

    void accumulate( Stats stats )
    {
        String statsKey = stats.key();
        if ( this.key == null )
        {
            this.key = statsKey;
            this.operation = stats.operation;
            this.version = stats.version;
            this.threadCount = stats.threadCount;
            this.objectSize = stats.objectSize;
        }

        if ( this.key.equals( statsKey ) )
        {
            this.successfulOperations += stats.successfulOperations;
            this.errorCount += stats.errorCount;
            this.fastestOperation = Math.min( stats.fastestOperation, this.fastestOperation );
            this.slowestOperation = Math.max( stats.slowestOperation, this.slowestOperation );
            this.elapsedMilliseconds += stats.elapsedMilliseconds;
            this.currentThreadCpuUsedMilliseconds += stats.currentThreadCpuUsedMilliseconds;
            this.processCpuUsedMilliseconds += stats.processCpuUsedMilliseconds;
            this.bytesRead += stats.bytesRead;
            
        }
    }
}

