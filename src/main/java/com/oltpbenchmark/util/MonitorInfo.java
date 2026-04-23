/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.oltpbenchmark.util;

import java.nio.file.Path;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface MonitorInfo {

  public enum MonitoringType {
    ADVANCED,
    THROUGHPUT,
    LATENCY;
  }

  /** Monitoring interval. */
  @Value.Default
  public default int getMonitoringInterval() {
    return 0;
  }

  /** Monitoring type. */
  @Value.Default
  public default MonitoringType getMonitoringType() {
    return MonitoringType.THROUGHPUT;
  }

  /**
   * Path to the CSV file where per-interval latency metrics should be appended. When empty, no CSV
   * is written (log lines are still emitted if the monitoring type requests them).
   */
  @Value.Default
  public default Optional<Path> getLatencyReportCsvPath() {
    return Optional.empty();
  }
}
