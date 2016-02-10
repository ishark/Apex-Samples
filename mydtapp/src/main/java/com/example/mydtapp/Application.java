/**
 * Put your copyright and license info here.
 */
package com.example.mydtapp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build
    // Application does not exit even if reading is done since FileInputReaderWithoutShutDownException does not throw exception
    DummyOutputOperator combinedConsole = dag.addOperator("console", new DummyOutputOperator());
    
    FileInputReaderWithShutDown inputOp = dag.addOperator("inputWordsReader", new FileInputReaderWithShutDown());
    inputOp.setDirectory("/media/isha/apex-samples/sampleInput1");
    dag.addStream("stream1", inputOp.output, combinedConsole.input1);
    
    FileInputReaderWithoutShutDownException inputOp1 = dag.addOperator("inputTextReader1", new FileInputReaderWithoutShutDownException());
    inputOp1.setDirectory("/media/isha/apex-samples/sampleInput2");
    dag.addStream("stream2", inputOp1.output, combinedConsole.input2);
  }
}
