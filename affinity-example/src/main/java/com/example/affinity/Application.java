/**
 * Put your copyright and license info here.
 */
package com.example.affinity;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.AffinityRule;
import com.datatorrent.api.AffinityRule.OperatorPair;
import com.datatorrent.api.AffinityRule.Type;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomNumberGenerator rand = dag.addOperator("rand", new RandomNumberGenerator());
    UniqueCounter<Double> counter = dag.addOperator("counter", new UniqueCounter<Double>());
    PassThruOperator passThru1 = dag.addOperator("passThru", new PassThruOperator());
    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("rand_calc", rand.out, counter.data).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("passthru1", counter.count, passThru1.input);
    dag.addStream("rand_console",passThru1.output, console.input);
    dag.setAttribute(passThru1, OperatorContext.PARTITIONER, new StatelessPartitioner<PassThruOperator>(10));
    
    List<AffinityRule> rules = new ArrayList<>();

    rules.add(new AffinityRule(Type.AFFINITY, new OperatorPair( "rand", "console"), Locality.NODE_LOCAL, false));
    rules.add(new AffinityRule(Type.ANTI_AFFINITY, new OperatorPair( "rand", "passThru1"), Locality.NODE_LOCAL, false));
    rules.add(new AffinityRule(Type.ANTI_AFFINITY, new OperatorPair( "passThru", "passThru"), Locality.NODE_LOCAL, false));
    
    dag.setAttribute(DAGContext.AFFINITY_RULES, rules);
    dag.getMeta(rand).getAttributes().put(OperatorContext.LOCALITY_HOST, "node17.morado.com");    
  }
}
