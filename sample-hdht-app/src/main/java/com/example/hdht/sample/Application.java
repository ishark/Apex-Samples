/**
 * Put your copyright and license info here.
 */
package com.example.hdht.sample;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.fileaccess.TFileImpl;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator.FileLineInputOperator;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build

    RandomKeyValGenerator randomGenerator = dag.addOperator("randomGenerator", new RandomKeyValGenerator());
    randomGenerator.setNumTuples(100000);
    HdhtStoreOperator store = dag.addOperator("hdht", new HdhtStoreOperator());
    store.setPartitionCount(4);
    String basePath = "/user/isha/hdht";
    TFileImpl hdsFile = new TFileImpl.DTFileImpl();
    basePath += "/" + System.currentTimeMillis();
    hdsFile.setBasePath(basePath);
    System.out.println("Setting basePath " + basePath);
    store.setFileStore(hdsFile);
    
//    ConsoleOutputOperator cons = dag.addOperator("console", new ConsoleOutputOperator());

    dag.addStream("randomData", randomGenerator.out, store.input);//.setLocality(Locality.CONTAINER_LOCAL);
  }
}
