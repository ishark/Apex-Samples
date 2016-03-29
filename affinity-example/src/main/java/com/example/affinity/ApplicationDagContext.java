/**
 * Put your copyright and license info here.
 */
package com.example.affinity;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.AffinityRule;
import com.datatorrent.api.AffinityRule.Type;
import com.datatorrent.api.AffinityRulesSet;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="AffinityRulesFromDagContext")
public class ApplicationDagContext implements StreamingApplication
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
    
    
    AffinityRulesSet ruleSet = new AffinityRulesSet();
    
    List<AffinityRule> rules = new ArrayList<>();
    List<String> operators = new ArrayList<>();
    operators.add("passThru");
    operators.add("passThru");
       
    rules.add(new AffinityRule(Type.AFFINITY,"rand|console" , Locality.NODE_LOCAL, false));
    rules.add(new AffinityRule(Type.ANTI_AFFINITY, Locality.NODE_LOCAL, false, "rand", "passThru"));
    rules.add(new AffinityRule(Type.ANTI_AFFINITY, operators, Locality.NODE_LOCAL, false));
    ruleSet.setAffinityRules(rules);
    
    dag.setAttribute(DAGContext.AFFINITY_RULES_SET, ruleSet);

    dag.getMeta(rand).getAttributes().put(OperatorContext.LOCALITY_HOST, "node17.morado.com");    
  }
}
