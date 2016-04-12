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

@ApplicationAnnotation(name="WriteAndQueryApplication")
public class WriteAndQueryHDHT implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build
    AscendingKeyValGenerator keyValGenerator = dag.addOperator("keyValGenerator", new AscendingKeyValGenerator());
    keyValGenerator.setNumTuples(1000);
   
    AscendingKeyGenerator generator = dag.addOperator("keyGenerator", new AscendingKeyGenerator());
    generator.setNumTuples(100);
    HdhtStoreOperator store = dag.addOperator("hdht", new HdhtStoreOperator());
    store.setPartitionCount(2);
    store.setNumberOfBuckets(14);
    String basePath = "/user/isha/hdht/testWriteAndQuery";
    TFileImpl hdsFile = new TFileImpl.DTFileImpl();

    hdsFile.setBasePath(basePath);
    System.out.println("Setting basePath " + basePath);
    store.setFileStore(hdsFile);

    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("store input", keyValGenerator.out, store.input);
    dag.addStream("randomData", generator.out, store.queryInput);//.setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("output", store.queryOutput, console.input);
  }
}
