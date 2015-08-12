package com.scalyr.s3performance;

import java.lang.IllegalArgumentException;
import java.nio.IntBuffer;
import java.util.Random;

class RandomIdBuffer
{
    private int[] objectIds;

    private int blockSize;
    private int totalBlocks;

    public RandomIdBuffer( int totalObjects )
    {
        this( totalObjects, System.currentTimeMillis() );
    }

    public RandomIdBuffer( int totalObjects, long seed )
    {
        this.objectIds = new int[totalObjects];

        populateList( this.objectIds );
        shuffleList( this.objectIds, seed );

        this.totalBlocks = 1;
        this.blockSize = totalObjects;
    }

    public void setBlockCount( int blockCount ) throws IllegalArgumentException
    {
        int remainder = this.objectIds.length % blockCount;
        if ( remainder != 0 )
        {
            throw new IllegalArgumentException( "Block count " + blockCount + " does not divide evenly in to total size " + this.objectIds.length + "." );
        }
        this.totalBlocks = blockCount;
        this.blockSize = this.objectIds.length / blockCount;
    }

    public int totalBlocks()
    {
        return this.totalBlocks;
    }

    IntBuffer getBlock( int blockNum )
    {
        int offset = blockNum * blockSize;
        int length = blockSize;
        return IntBuffer.wrap( this.objectIds, offset, length );
    }

    private void populateList( int[] list )
    {
        for ( int i = 0; i < list.length; ++i )
        {
            list[i] = i;
        }
    }

    private void shuffleList( int[] list, long seed )
    {
        Random random = new Random( seed );

        //Modern Fisher-Yates shuffle: https://en.wikipedia.org/wiki/Fisher–Yates_shuffle#The_modern_algorithm
        for ( int i = list.length - 1; i > 0; --i )
        {
           int j = random.nextInt( i + 1 ); 
           int temp = list[j];
           list[j] = list[i];
           list[i] = temp;
        }
    }
}

