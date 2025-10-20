package com.chinagoods.bigdata.domain;

import java.io.Serializable;

/**
 * Accumulator for WeightedAvg.
 * @author xiaowei.song
 */
public class WeightedAvgAccum implements Serializable {
  private static final long serialVersionUID = 1L;

  public long sum = 0;
  public int count = 0;
}
