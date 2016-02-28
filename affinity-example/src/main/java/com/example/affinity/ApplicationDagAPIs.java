/**
 * Put your copyright and license info here.
 */
package com.example.affinity;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="AffinityRulesDagAPIs")
public class ApplicationDagAPIs implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomNumberGenerator rand = dag.addOperator("rand", new RandomNumberGenerator());
    UniqueCounter<Double> counter = dag.addOperator("counter", new UniqueCounter<Double>());
    PassThruOperator passThru1 = dag.addOperator("passThru", new PassThruOperator());
    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("rand_calc", rand.out, counter.data).setLocality(Locality.NODE_LOCAL);
    dag.addStream("passthru1", counter.count, passThru1.input);
    dag.addStream("rand_console",passThru1.output, console.input);
    dag.setAttribute(passThru1, OperatorContext.PARTITIONER, new StatelessPartitioner<PassThruOperator>(10));
    
    dag.setAffinity(Locality.CONTAINER_LOCAL, false, "rand", "console");
    dag.setAntiAffinity(Locality.NODE_LOCAL, false, "rand", "passThru", "passThru");
   
    dag.getMeta(rand).getAttributes().put(OperatorContext.LOCALITY_HOST, "node17.morado.com");    
  }
}
