package com.example.affinity;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class PassThruOperator extends BaseOperator
{
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object tuple)
    {
      output.emit(tuple);
    }
  };
  public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<Object>();
}
