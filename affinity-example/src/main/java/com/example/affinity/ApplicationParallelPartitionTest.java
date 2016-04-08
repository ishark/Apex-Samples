/**
 * Put your copyright and license info here.
 */
package com.example.affinity;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.AffinityRule.Type;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.AffinityRule;
import com.datatorrent.api.AffinityRulesSet;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name = "ApplicationParallelPartitionTest")
public class ApplicationParallelPartitionTest implements StreamingApplication
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
    dag.addStream("rand_console", passThru1.output, console.input);
    dag.setAttribute(rand, OperatorContext.PARTITIONER, new StatelessPartitioner<RandomNumberGenerator>(5));

    AffinityRulesSet ruleSet = new AffinityRulesSet();
    List<AffinityRule> rules = new ArrayList<>();
    rules.add(new AffinityRule(Type.ANTI_AFFINITY, Locality.NODE_LOCAL, false, "rand", "rand"));
    ruleSet.setAffinityRules(rules);

    dag.setAttribute(DAGContext.AFFINITY_RULES_SET, ruleSet);
    dag.setInputPortAttribute(counter.data, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(passThru1.input, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(console.input, PortContext.PARTITION_PARALLEL, true);

  }
}
