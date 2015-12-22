package com.example.neuralnet;

import java.util.ArrayList;

public class LayerOutputFormat
{
  @Override
  public String toString()
  {
    return "LayerOutputFormat [outputs=" + outputs + ", finalTargets=" + finalTargets + "]";
  }

  public ArrayList<Double> outputs;
  public ArrayList<Double> finalTargets;

  LayerOutputFormat(ArrayList<Double> outputs, ArrayList<Double> finalTargets)
  {
    this.outputs = outputs;
    this.finalTargets = finalTargets;
  }

  public LayerOutputFormat()
  {
  }
}
