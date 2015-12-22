package com.example.neuralnet;

import java.util.ArrayList;

import com.datatorrent.api.DefaultOutputPort;

public class OutputLayerOperator extends LayerOperator
{
  private double threshold = 0.0001;

  public OutputLayerOperator(int inputPerNode, int numberOfNodes, double[][] incomingWeights, double eta, double[] biases)
  {
    super(inputPerNode, numberOfNodes, incomingWeights, eta, biases);
  }

  public OutputLayerOperator()
  {
    super();
  }
  public final transient DefaultOutputPort<ArrayList<Double>> deltaOut = new DefaultOutputPort<ArrayList<Double>>();

  @Override
  public LayerOutputFormat processInput(LayerOutputFormat input)
  {
    LayerOutputFormat out = super.processInput(input);
    
    computeDeltaWeights(input, out);
    
    return out;
  }

  private double calculateError(LayerOutputFormat input, LayerOutputFormat out)
  {
    double squaredSum = 0;
    for (int i = 0; i < out.finalTargets.size(); i++) {
      double diff = out.finalTargets.get(i) - out.outputs.get(i);
      squaredSum += diff * diff / 2;
    }
    return squaredSum;
  }

  private void computeDeltaWeights(LayerOutputFormat input, LayerOutputFormat out)
  { 
    ArrayList<Double> deltas = new ArrayList<Double>();
    double error = calculateError(input, out);
    logger.debug("error value = {}", error);
  
    if (error > getThreshold()) {
    for (int i = 0; i < numberOfNodes; i++) {
      double deltaEDerivative = (out.outputs.get(i) - out.finalTargets.get(i));
      double deltaOutDerivative = out.outputs.get(i) * (1 - out.outputs.get(i));
      double delta = deltaEDerivative * deltaOutDerivative;
      deltas.add(delta);
      deltaBiases[i] += eta * delta;
      for (int j = 0; j < inputsPerNode; j++) {
        deltaWeights[i][j] += eta * delta * input.outputs.get(j);
      }
    }
    } else {
      for (int i = 0; i < numberOfNodes; i++) {
        deltas.add(0.0);
      }
    }
    logger.debug("Emitting delta values {}", deltas);
    deltaOut.emit(deltas);
  }

  public double getThreshold()
  {
    return threshold;
  }

  public void setThreshold(double threshold)
  {
    this.threshold = threshold;
  }
}
