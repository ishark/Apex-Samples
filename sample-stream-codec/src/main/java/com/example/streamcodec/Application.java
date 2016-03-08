/**
 * Put your copyright and license info here.
 */
package com.example.streamcodec;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="StreamCodecSample")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build

    FileInputLineReader input = dag.addOperator("input", new FileInputLineReader());
    
    LogCollector collector = dag.addOperator("collector", new LogCollector());

    SampleStreamCodec codec = new SampleStreamCodec();
    codec.setFilterByClass("com.datatorrent.stram.StreamingAppMasterService");
    dag.setInputPortAttribute(collector.input, PortContext.STREAM_CODEC, codec);
    dag.addStream("filteredStream", input.output, collector.input);
  }
}
