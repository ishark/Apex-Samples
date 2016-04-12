/**
 * Put your copyright and license info here.
 */
package com.example.hdht.sample;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.fileaccess.TFileImpl;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="HdhtQueryTestApplication")
public class TestQueryApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build

    AscendingKeyValGenerator generator = dag.addOperator("keyValGenerator", new AscendingKeyValGenerator());
    generator.setNumTuples(100000);
    HdhtStoreOperator store = dag.addOperator("hdht", new HdhtStoreOperator());
    store.setPartitionCount(3);
//    store.setNumberOfBuckets(12);
    String basePath = "/user/isha/hdht/test";
    TFileImpl hdsFile = new TFileImpl.DTFileImpl();
//    basePath += "/" + System.currentTimeMillis();
    hdsFile.setBasePath(basePath);
    System.out.println("Setting basePath " + basePath);
    store.setFileStore(hdsFile);

    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("randomData", generator.out, store.input);//.setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("output", store.queryOutput, console.input);
  }
}
