package com.scalyr.s3performance;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

class Timer
{
    private long startTime;
    private long cpuStartTime;
    private long elapsedMs;
    private long cpuElapsedMs;

    private ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    public void start()
    {
        this.startTime = System.currentTimeMillis();
        this.cpuStartTime = threadBean.getCurrentThreadCpuTime();
    }

    public void stop()
    {
        this.cpuElapsedMs = threadBean.getCurrentThreadCpuTime() - this.cpuStartTime;
        this.elapsedMs = System.currentTimeMillis() - this.startTime;
    }

    public long elapsedMilliseconds()
    {
        return this.elapsedMs;
    }

    public long cpuUsedMilliseconds()
    {
        return this.cpuElapsedMs;
    }
}

