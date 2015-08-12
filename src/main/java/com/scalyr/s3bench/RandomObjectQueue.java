package com.scalyr.s3bench;

import java.nio.IntBuffer;

import org.apache.commons.codec.digest.DigestUtils;

class RandomObjectQueue
{
    private IntBuffer intBuffer;
    public RandomObjectQueue( IntBuffer intBuffer )
    {
        this.intBuffer = intBuffer;
    }

    public boolean hasRemaining()
    {
        return this.intBuffer.hasRemaining();
    }

    public String nextObject()
    {
        String result = null;
        if ( this.intBuffer.hasRemaining() )
        {
            int objectId = this.intBuffer.get();
            result = DigestUtils.sha1Hex( Integer.toString( objectId ) );
        }
        return result;
    }

}
