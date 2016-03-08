package com.example.streamcodec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.google.common.collect.Sets;

public class LogCollector extends BaseOperator implements Partitioner<LogCollector>
{
  public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
  {

    @Override
    public void process(String tuple)
    {
      System.out.println(tuple);
      if(output.isConnected()) {
        output.emit(tuple);
      }
    }
  }; 

  @OutputPortFieldAnnotation(optional=true)
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
  @Override
  public void partitioned(Map arg0)
  {
    // TODO Auto-generated method stub
  }

  @Override
  public Collection<Partition<LogCollector>> definePartitions(Collection<Partition<LogCollector>> arg0, PartitioningContext arg1)
  {
    Collection<Partition<LogCollector>> newPartitions = new ArrayList<Partition<LogCollector>>();

    int partitionMask = 0x03;

    // No partitioning done so far..
    // Single partition with mask 0x03 and set {0}
    // First partition
    // This will pick tuples only if stream codec getPartition returns 0
    LogCollector newInstance = new LogCollector();
    Partition<LogCollector> partition = new DefaultPartition<LogCollector>(newInstance);
    HashSet<Integer> set = Sets.newHashSet();
    set.add(0);
    PartitionKeys value = new PartitionKeys(partitionMask, set);
    partition.getPartitionKeys().put(input, value);
    newPartitions.add(partition);

    return newPartitions;
  }
  
}
