package com.example.mydtapp;


/**
 * Put your copyright and license info here.
 */

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;

@ApplicationAnnotation(name="AutoShutDownApplication")
public class ApplicationWithAutoShutDown implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build
    // Application does auto exits when input is done since both input reader operators throw shutdown exception when done.
    DummyOutputOperator combinedConsole = dag.addOperator("console", new DummyOutputOperator());
    
    FileInputReaderWithShutDown inputOp = dag.addOperator("inputWordsReader", new FileInputReaderWithShutDown());
    inputOp.setDirectory("/media/isha/apex-samples/sampleInput1");
    dag.addStream("stream1", inputOp.output, combinedConsole.input1);
    
    FileInputReaderWithShutDown inputOp1 = dag.addOperator("inputTextReader1", new FileInputReaderWithShutDown());
    inputOp1.setDirectory("/media/isha/apex-samples/sampleInput2");
    dag.addStream("stream2", inputOp1.output, combinedConsole.input2);
  }
}
