package com.example.affinity;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.AffinityRule;
import com.datatorrent.api.AffinityRulesSet;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.AffinityRule.Type;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name = "AffinityRulesContainerLocal")
public class ApplicationTestStreamLocal implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomNumberGenerator rand = dag.addOperator("rand", new RandomNumberGenerator());
    UniqueCounter<Double> counter = dag.addOperator("counter", new UniqueCounter<Double>());
    PassThruOperator passThru1 = dag.addOperator("passThru", new PassThruOperator());
    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
    ConsoleOutputOperator console1 = dag.addOperator("console1", new ConsoleOutputOperator());

    dag.addStream("rand_calc", rand.out, counter.data, passThru1.input).setLocality(Locality.NODE_LOCAL);

    dag.addStream("rand_console", passThru1.output, console.input);
    dag.addStream("counter_console", counter.count, console1.input);

    dag.setAttribute(passThru1, OperatorContext.PARTITIONER, new StatelessPartitioner<PassThruOperator>(5));

    AffinityRulesSet ruleSet = new AffinityRulesSet();
    List<AffinityRule> rules = new ArrayList<>();
    rules.add(new AffinityRule(Type.AFFINITY, Locality.CONTAINER_LOCAL, false, "rand", "console", "counter"));
    ruleSet.setAffinityRules(rules);

    dag.setAttribute(DAGContext.AFFINITY_RULES_SET, ruleSet);

    dag.getMeta(rand).getAttributes().put(OperatorContext.LOCALITY_HOST, "node17.morado.com");
  }
}
