package com.example.neuralnet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class InputLayerOperator extends BaseOperator implements InputOperator, Serializable
{

  private long startWindowId = 0; 
  public final transient DefaultOutputPort<LayerOutputFormat> output = new DefaultOutputPort<LayerOutputFormat>();
  private List<LayerOutputFormat> emitList = new LinkedList<LayerOutputFormat>();

  public final transient DefaultOutputPort<Boolean> updateWeightsSignal = new DefaultOutputPort<Boolean>();
  private int nextIndex;
  
  @Override
  public void emitTuples()
  {
//    emit();
  }

  private void emit()
  {
//    for (LayerOutputFormat inputSequence : emitList) {
      output.emit(emitList.get(nextIndex++));
      if(nextIndex >= emitList.size()) {
        nextIndex = 0;
      }
//    }
  }

  public InputLayerOperator(int inputCount, List<LayerOutputFormat> inputList)
  {
    this.emitList = inputList;
  }

  public InputLayerOperator()
  {

  }
  
  @Override
  public void beginWindow(long windowId)
  { 
    super.beginWindow(windowId);
    if(startWindowId == 0) {
      nextIndex = 0;
      startWindowId = windowId;
      emit();
    } else if((windowId - startWindowId) % 2 == 0) {
      System.out.println("Send update signal....");
      updateWeightsSignal.emit(true);
    } else {
      emit();
    }
  }
}
