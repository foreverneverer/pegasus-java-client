package com.xiaomi.infra.pegasus.tools;

import com.xiaomi.infra.pegasus.operator.client_operator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;

public class LatencyTracer {
  int id;
  long startTime;
  long lastTime;
  long endTime;
  String requestType;
  Map<String, Long> points = new LinkedHashMap<>();

  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(client_operator.class);

  public LatencyTracer() {
    startTime = System.nanoTime();
    lastTime = startTime;
  }

  public void addPoint(String name, long currentTime) {
    points.put(name, currentTime - lastTime);
    lastTime = currentTime;
  }

  public void report(long timeOut) {
    long timeUsed = endTime - startTime;
    boolean success = true;
    if (timeUsed / 1000000 >= timeOut) {
      success = false;
    }
    String log =
        String.format(
            "trace log, id=%s, startTime=%s, timeUsed=%s, success=%s",
            id, startTime, endTime - startTime, success);
    for (Map.Entry<String, Long> point : points.entrySet()) {
      log =
          String.format(
              "%s\n\tTRACER[%s]: %s=%s", log, requestType, point.getKey(), point.getValue());
    }
    logger.info("{}", log);
  }

  public void setId(int id) {
    this.id = id;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public void setRequestType(String requestType) {
    this.requestType = requestType;
  }
}
