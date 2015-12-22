package com.example.neuralnet;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.BaseOperator;

public class LayerOperator extends BaseOperator
{
  protected double[][] incomingWeights;
  protected double[][] deltaWeights;
  protected double[] deltaBiases;
  protected double[] biases;
  protected int inputsPerNode;
  protected int numberOfNodes;
  protected double eta;
  protected long processedCount = 0;
  protected boolean updateWeights = false;
  private int operatorId;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    operatorId = context.getId();
  }

  
  public final transient DefaultInputPort<LayerOutputFormat> input = new DefaultInputPort<LayerOutputFormat>()
  {

    @Override
    public void process(LayerOutputFormat input)
    {
      processInput(input);
    }
  };
  public final transient DefaultOutputPort<Boolean> relayUpdateWeightsSignal = new DefaultOutputPort<Boolean>();
    
  public final transient DefaultInputPort<Boolean> updateWeightsSignal = new DefaultInputPort<Boolean>()
  {

    @Override
    public void process(Boolean arg0)
    {
      if (arg0) {
        synchronized (incomingWeights) {
          if (processedCount > 0) {
            updateWeightMatrix(arg0);
          } else {
            updateWeights = true;
          }
        }
      }
    } 
  };

  public void updateWeightMatrix(Boolean arg0)
  {
    logger.debug("Update weight matrix for operator {}", operatorId);
    for (int i = 0; i < numberOfNodes; i++) {
      for (int j = 0; j < inputsPerNode; j++) {
        incomingWeights[i][j] -= (deltaWeights[i][j] / processedCount);
        relayUpdateWeightsSignal.emit(arg0);
      }
      biases[i] -= (deltaBiases[i] / processedCount);
    }
    resetDeltaWeightsAndBias();
    processedCount = 0;
  }
  private double activationFunction(Double weightedInputSum)
  {
    // Use sigmoid function as activation function
    double output = 1 / (1 + Math.pow(Math.E, -1 * weightedInputSum));
    return output;
  }

  public final transient DefaultOutputPort<LayerOutputFormat> output = new DefaultOutputPort<LayerOutputFormat>();

  public LayerOutputFormat processInput(LayerOutputFormat input)
  {
    synchronized (incomingWeights) {
      processedCount++;
      LayerOutputFormat out = new LayerOutputFormat();
      ArrayList<Double> outputArr = new ArrayList<Double>(numberOfNodes);
      for (int i = 0; i < numberOfNodes; i++) {
        double weightedInputSum = 0;
        int j = 0;
        for (j = 0; j < inputsPerNode; j++) {
          weightedInputSum += incomingWeights[i][j] * input.outputs.get(j);
        }
        weightedInputSum += biases[i];
        outputArr.add(activationFunction(weightedInputSum));
      }
      out.outputs = outputArr;
      out.finalTargets = input.finalTargets;
      logger.debug("Emitting output {} for operator {}", out, operatorId);
      output.emit(out);
      return out;
    }
  }

  public LayerOperator(int inputPerNode, int numberOfNodes, double[][] weights, double eta, double[] biases)
  {
    this.incomingWeights = weights;
    this.deltaWeights = new double[numberOfNodes][inputPerNode];
    this.deltaBiases = new double[numberOfNodes];
    this.inputsPerNode = inputPerNode;
    this.numberOfNodes = numberOfNodes;
    this.eta = eta;
    resetDeltaWeightsAndBias();
    this.biases = biases;
  }

  protected void resetDeltaWeightsAndBias()
  {
    for (int i = 0; i < numberOfNodes; i++) {
      for (int j = 0; j < inputsPerNode; j++) {
        deltaWeights[i][j] = 0;
      }
      deltaBiases[i] = 0;
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    synchronized (incomingWeights) {
      if(updateWeights && processedCount > 0) {
        updateWeightMatrix(true);
        updateWeights = false;
      }
    }
  }

  public LayerOperator()
  {
  }

  public double getEta()
  {
    return eta;
  }

  public void setEta(double eta)
  {
    this.eta = eta;
  }

  public static Logger logger = LoggerFactory.getLogger(LayerOperator.class);
}
