package com.example.hdht.sample;

import java.util.Random;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;

public class AscendingKeyValGenerator extends BaseOperator implements InputOperator
{
  private int numTuples = 100;
  private transient int count = 0;
  private int index = 0;
  public final transient DefaultOutputPort<KeyValPair<Integer, Integer>> out = new DefaultOutputPort<KeyValPair<Integer, Integer>>();

  private int maxKey = 1000000;
  
  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
  }

  @Override
  public void emitTuples()
  {
    if (count++ < numTuples || index == Integer.MAX_VALUE) {
      out.emit(new KeyValPair<Integer,Integer>(index, index++));
    }
  }

  public int getNumTuples()
  {
    return numTuples;
  }

  /**
   * Sets the number of tuples to be emitted every window.
   * @param numTuples number of tuples
   */
  public void setNumTuples(int numTuples)
  {
    this.numTuples = numTuples;
  }

  public int getMaxKey()
  {
    return maxKey;
  }

  public void setMaxKey(int maxKey)
  {
    this.maxKey = maxKey;
  }

}
