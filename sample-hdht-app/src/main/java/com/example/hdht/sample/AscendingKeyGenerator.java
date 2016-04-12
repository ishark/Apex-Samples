package com.example.hdht.sample;


import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class AscendingKeyGenerator extends BaseOperator implements InputOperator
{
  private int numTuples = 10;
  private transient int count = 0;
  private int index = 0;
  public final transient DefaultOutputPort<Integer> out = new DefaultOutputPort<Integer>();
  boolean emitTuples = false;
  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
    if (windowId % 2 == 0) {
      emitTuples = true;
    }
  }

  @Override
  public void emitTuples()
  {
    if (emitTuples && (count++ < numTuples || index == Integer.MAX_VALUE)) {
      out.emit(index++);
    }
  }

  public int getNumTuples()
  {
    return numTuples;
  }


  @Override
  public void endWindow()
  {
    super.endWindow();
    emitTuples = false;
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
