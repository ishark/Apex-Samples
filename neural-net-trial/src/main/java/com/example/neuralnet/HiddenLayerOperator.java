package com.example.neuralnet;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

public class HiddenLayerOperator extends LayerOperator
{

  private ConcurrentLinkedQueue<InputOutputMap> inputsReceived = new ConcurrentLinkedQueue<InputOutputMap>();
  protected double[][] outputGoingWeights;
  protected double[][] outputGoingWeightDeltas;
  protected int outputsPerNode;
      
  public final transient DefaultInputPort<ArrayList<Double>> deltaInput = new DefaultInputPort<ArrayList<Double>>()
  {
    @Override
    public void process(ArrayList<Double> deltas)
    {
      computeDeltaWeights(deltas);
    }
  };

  @Override
  public void updateWeightMatrix(Boolean arg0) 
  {
    for(int i =0 ; i < numberOfNodes; i++) {
      for(int j =0 ; j < outputsPerNode; j++) {
        outputGoingWeights[j][i] -= outputGoingWeightDeltas[j][i];
      }
    }
    super.updateWeightMatrix(arg0);
    resetOutputDeltaWeights();
  }

  public HiddenLayerOperator(int inputPerNode, int numberOfNodes, double[][] incomingWeights, double eta, double[][] outgoingWeights, int outputsPerNode, double[] biases)
  { 
    super(inputPerNode, numberOfNodes, incomingWeights, eta, biases);
    this.outputGoingWeights = outgoingWeights;
    this.outputsPerNode = outputsPerNode;
    this.outputGoingWeightDeltas = new double[outputsPerNode][numberOfNodes];
    resetOutputDeltaWeights();
  }
  public HiddenLayerOperator()
  {
    super();
  }

  protected void resetOutputDeltaWeights()
  {
    for (int i = 0; i < numberOfNodes; i++) {
      for (int j = 0; j < outputsPerNode; j++) {
        outputGoingWeightDeltas[j][i] = 0;
      }
    }
  }
  private void computeDeltaWeights(ArrayList<Double> deltas)
  {
    // TODO Auto-generated method stub
    if (!inputsReceived.isEmpty()) {
      InputOutputMap map = inputsReceived.poll();
      if (map != null) {
        for (int i = 0; i < numberOfNodes; i++) {

          double sumDeltas = 0;
          for (int j = 0; j < outputsPerNode; j++) {
            sumDeltas += outputGoingWeights[j][i] * deltas.get(j);
            outputGoingWeightDeltas[j][i] += eta * deltas.get(j) * map.outputs.get(j);
          }
          double deltaH = sumDeltas * map.outputs.get(i) * (1 - map.outputs.get(i));
          deltaBiases[i] += eta *deltaH;
          for (int j = 0; j < inputsPerNode; j++) {
            deltaH *= map.inputs.get(j);
            deltaWeights[i][j] += eta * deltaH;
          }
        }
      }
    }
  }

  @Override
  public LayerOutputFormat processInput(LayerOutputFormat input)
  {
    InputOutputMap map = new InputOutputMap();
    map.inputs = input.outputs;
    LayerOutputFormat out = super.processInput(input);
    map.outputs = out.outputs;
    inputsReceived.add(map);
    
    return out;
  }
}
