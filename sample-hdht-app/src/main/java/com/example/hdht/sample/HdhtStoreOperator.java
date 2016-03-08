package com.example.hdht.sample;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.netlet.util.Slice;

public class HdhtStoreOperator extends AbstractSinglePortHDHTWriter<KeyValPair<Integer, Integer>>
{

  @Override
  protected com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter.HDHTCodec<KeyValPair<Integer, Integer>> getCodec()
  {
    return new HdhtSampleCodec();
  }

  public static class HdhtSampleCodec extends KryoSerializableStreamCodec<KeyValPair<Integer, Integer>> implements HDHTCodec<KeyValPair<Integer, Integer>> {
    
    @Override
    public int getPartition(KeyValPair<Integer, Integer> arg0)
    {
      return arg0.getKey().hashCode();
    }

    
    @Override
    public KeyValPair<Integer, Integer> fromKeyValue(Slice key, byte[] value)
    {
      return new KeyValPair<Integer, Integer>(convertBytes(key.toByteArray()), convertBytes(value));
    }

    @Override
    public byte[] getKeyBytes(KeyValPair<Integer, Integer> arg0)
    {
      return getBytes(arg0.getKey());
    }

    @Override
    public byte[] getValueBytes(KeyValPair<Integer, Integer> arg0)
    {
      return getBytes(arg0.getValue());      
    }


    private byte[] getBytes(int i)
    {
      byte[] result = new byte[4];
      result[0] = (byte) (i >> 24);
      result[1] = (byte) (i >> 16);
      result[2] = (byte) (i >> 8);
      result[3] = (byte) (i /*>> 0*/);
      return result;
    }
    
    private int convertBytes(byte []bytes)
    {
      return ByteBuffer.wrap(bytes).getInt();
    }
  }
}

 