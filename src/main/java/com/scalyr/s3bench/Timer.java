package com.scalyr.s3bench;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

class Timer
{
    private double NANOS_TO_MILLIS = 1e-6;
    private long startTime;
    private long currentThreadCpuStartTime;
    private long processCpuStartTime;
    private long elapsedMs;
    private long currentThreadCpuElapsedNs;
    private long processCpuElapsedNs;

    private ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    private OperatingSystemMXBean systemBean = (OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();

    public void start()
    {
        this.processCpuStartTime = systemBean.getProcessCpuTime();
        this.currentThreadCpuStartTime = threadBean.getCurrentThreadCpuTime();
        this.startTime = System.currentTimeMillis();
    }

    public void stop()
    {
        this.elapsedMs = (System.currentTimeMillis() - this.startTime) ;
        this.currentThreadCpuElapsedNs = threadBean.getCurrentThreadCpuTime() - this.currentThreadCpuStartTime;
        this.processCpuElapsedNs = systemBean.getProcessCpuTime() - this.processCpuStartTime;
    }

    public long elapsedMilliseconds()
    {
        return this.elapsedMs;
    }

    public double currentThreadCpuUsedMilliseconds()
    {
        return this.currentThreadCpuElapsedNs * NANOS_TO_MILLIS;
    }

    public long currentThreadCpuUsedNanoseconds()
    {
        return this.currentThreadCpuElapsedNs;
    }

    public double processCpuUsedMilliseconds()
    {
        return this.processCpuElapsedNs * NANOS_TO_MILLIS;
    }

    public long processCpuUsedNanoseconds()
    {
        return this.processCpuElapsedNs;
    }
}

