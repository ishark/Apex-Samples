package com.example.hdht.sample;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.netlet.util.Slice;
import com.google.common.base.Throwables;

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
  }


  public static byte[] getBytes(int i)
  {
    byte[] result = new byte[4];
    result[0] = (byte) (i >> 24);
    result[1] = (byte) (i >> 16);
    result[2] = (byte) (i >> 8);
    result[3] = (byte) (i /*>> 0*/);
    return result;
  }
  
  private static int convertBytes(byte []bytes)
  {
    return ByteBuffer.wrap(bytes).getInt();
  }

  public transient final DefaultOutputPort<KeyValPair<Integer, Integer>> queryOutput = new DefaultOutputPort<KeyValPair<Integer, Integer>>();
      
  @InputPortFieldAnnotation (optional = true)
  public transient final DefaultInputPort<Integer> queryInput = new DefaultInputPort<Integer>()
  {
    @Override
    public void process(Integer tuple)
    {
      byte[] value = getUncommitted(getBucket(tuple), getKeyBytes(tuple));
      if(value == null) {
       try {
        value = get(getBucket(tuple), getKeyBytes(tuple));
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
      }
      if (value != null) {
        queryOutput.emit(new KeyValPair<Integer, Integer>(tuple, convertBytes(value)));
      } else {
        
      }
    }
  };
  
  public long getBucket(Integer tuple) 
  {
    return tuple.hashCode() & this.partitionMask;
  }
  
  public Slice getKeyBytes(Integer tuple) {
    return new Slice(getBytes(tuple));
  }
  
  public static Logger LOG = LoggerFactory.getLogger(HdhtStoreOperator.class);
}

 