package com.example.mydtapp;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

public class DummyOutputOperator extends BaseOperator
{
  public final transient DefaultInputPort<String> input1 = new DefaultInputPort<String>()
  {

    @Override
    public void process(String tuple)
    {
      // Print to console
      System.out.println("Input 1 = " + tuple);
    }
  };
  
  public final transient DefaultInputPort<String> input2 = new DefaultInputPort<String>()
  {

    @Override
    public void process(String tuple)
    {
      // Print to console
      System.out.println("Input 2 = " + tuple);
    }
  };
}
