package com.example.hdht.sample;

import java.util.Random;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;

public class AscendingKeyGenerator extends BaseOperator implements InputOperator
{
  private int numTuples = 10;
  private transient int count = 0;
  private int index = 0;
  public final transient DefaultOutputPort<Integer> out = new DefaultOutputPort<Integer>();

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
  }

  @Override
  public void emitTuples()
  {
    if (count++ < numTuples || index == Integer.MAX_VALUE) {
      out.emit(index++);
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
}
