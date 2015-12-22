/**
 * Put your copyright and license info here.
 */
package com.example.neuralnet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.SimpleDelayOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name = "MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    List<LayerOutputFormat> inputs = new LinkedList<LayerOutputFormat>();
    inputs.add(new LayerOutputFormat(new ArrayList<Double>(Arrays.asList(0.0, 0.0)), new ArrayList<Double>(Arrays
        .asList(0.0))));
    inputs.add(new LayerOutputFormat(new ArrayList<Double>(Arrays.asList(0.0, 1.0)), new ArrayList<Double>(Arrays
        .asList(1.0))));
    inputs.add(new LayerOutputFormat(new ArrayList<Double>(Arrays.asList(1.0, 0.0)), new ArrayList<Double>(Arrays
        .asList(0.0))));
    inputs.add(new LayerOutputFormat(new ArrayList<Double>(Arrays.asList(1.0, 1.0)), new ArrayList<Double>(Arrays
        .asList(1.0))));

    InputLayerOperator input = dag.addOperator("inputLayer", new InputLayerOperator(2, inputs));
    double[][] hiddenLayerWeightsMatrix = new double[][] { { 1, 1 }, { 1, 1 }, { 1, 1 } };
    double[][] outputLayerWeights = new double[][] { { 1, 1, 1 } };
    double[] hiddenLayerBias = {1, 1, 1};
    double[] outputLayerBias = {1, 1};
    double learningRate =2;
    HiddenLayerOperator hiddenLayer = dag.addOperator("hiddenLayer", new HiddenLayerOperator(2, 3, hiddenLayerWeightsMatrix, learningRate, outputLayerWeights, 1, hiddenLayerBias));
    OutputLayerOperator outputLayer = dag.addOperator("outputLayer", new OutputLayerOperator(3, 1, outputLayerWeights, learningRate, outputLayerBias));

    dag.addStream("inputStream", input.output, hiddenLayer.input);
    dag.addStream("hiddenLayerStream", hiddenLayer.output, outputLayer.input);

    SimpleDelayOperator<ArrayList<Double>> delayOp = dag.addOperator("delayOperator",
        new SimpleDelayOperator<ArrayList<Double>>());

    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("stream", outputLayer.output, console.input);
    dag.addStream("loopStream", outputLayer.deltaOut, delayOp.input);

    dag.addStream("loopBackStream", delayOp.output, hiddenLayer.deltaInput);
    
    // Add update weights signal streams
    dag.addStream("updateStream", input.updateWeightsSignal, hiddenLayer.updateWeightsSignal);
    dag.addStream("updateOutputWeights", hiddenLayer.relayUpdateWeightsSignal, outputLayer.updateWeightsSignal);

  }
}
