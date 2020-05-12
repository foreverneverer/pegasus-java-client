// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.rpc.async;

import com.xiaomi.infra.pegasus.metrics.MetricsManager;
import com.xiaomi.infra.pegasus.operator.client_operator;
import com.xiaomi.infra.pegasus.rpc.Table;
import java.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;

/** Created by weijiesun on 16-11-25. */
public final class ClientRequestRound {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(TableHandler.class);

  client_operator operator;
  Table.ClientOPCallback callback;
  long timeoutMs;

  boolean enableCounter;
  long createNanoTime;
  long expireNanoTime;
  boolean isCompleted;
  ScheduledFuture<?> backupRequestTask;

  /**
   * Constructor.
   *
   * @param op
   * @param enableCounter whether enable counter.
   */
  public ClientRequestRound(
      client_operator op,
      Table.ClientOPCallback cb,
      boolean enableCounter,
      long timeoutInMilliseconds) {
    this(
        op,
        cb,
        enableCounter,
        System.nanoTime() + timeoutInMilliseconds * 1000000L,
        timeoutInMilliseconds);
  }

  public ClientRequestRound(
      client_operator op,
      Table.ClientOPCallback cb,
      boolean enableCounter,
      long expireNanoTime,
      long timeoutInMilliseconds) {
    this.operator = op;
    this.callback = cb;
    this.timeoutMs = timeoutInMilliseconds;

    this.enableCounter = enableCounter;
    this.expireNanoTime = expireNanoTime;
    this.isCompleted = false;
    this.backupRequestTask = null;
  }

  public com.xiaomi.infra.pegasus.operator.client_operator getOperator() {
    return operator;
  }

  public void setOperator(client_operator op) {
    operator = op;
  }

  public void thisRoundCompletion() {
    try {
      callback.onCompletion(operator);
    } catch (Throwable ex) {
      // The exception is generated by the user's callback logic, so we don't do much things on it
      logger.debug("{} got exception", operator.toString(), ex);
    }

    if (enableCounter) {
      MetricsManager.updateCount(operator.getQPSCounter(), 1L);
      MetricsManager.setHistogramValue(
          operator.getLatencyCounter(), System.nanoTime() - createNanoTime);
    }
  }
}
