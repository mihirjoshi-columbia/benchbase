/*
 * Copyright 2026 by OLTPBenchmark Project
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
 */

package com.oltpbenchmark.api.collectors.monitoring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.oltpbenchmark.BenchmarkState;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.MockBenchmark;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.MonitorInfo;
import com.oltpbenchmark.util.MonitorInfo.MonitoringType;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link Monitor} latency reporting (CSV + aggregation logic).
 *
 * <p>We avoid starting the monitor thread; instead we exercise the protected hooks {@link
 * Monitor#initLatencyReportingIfEnabled()} and {@link Monitor#reportLatencyTick(int, double)}
 * directly and inspect the resulting CSV.
 */
public class TestMonitor {

  private static final class SeededWorker extends Worker<MockBenchmark> {
    SeededWorker(MockBenchmark bench, int id) {
      super(bench, id);
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType txnType) {
      return TransactionStatus.SUCCESS;
    }
  }

  private static final class TestMonitorImpl extends Monitor {
    TestMonitorImpl(
        MonitorInfo info,
        BenchmarkState state,
        List<? extends Worker<? extends BenchmarkModule>> workers) {
      super(info, state, workers);
    }

    // Expose protected hooks to the test.
    void initForTest() {
      initLatencyReportingIfEnabled();
    }

    void tickForTest(int intervalMs, double tps) {
      reportLatencyTick(intervalMs, tps);
    }
  }

  private Path tempCsv;
  private BenchmarkState benchmarkState;

  @Before
  public void setUp() throws IOException {
    this.tempCsv = Files.createTempFile("bb-latency-intervals-", ".csv");
    this.benchmarkState = new BenchmarkState(1);
  }

  @After
  public void tearDown() throws IOException {
    if (tempCsv != null) {
      Files.deleteIfExists(tempCsv);
    }
  }

  private static SeededWorker makeWorker() {
    WorkloadConfiguration workConf = new WorkloadConfiguration();
    workConf.setBenchmarkName("mockbenchmark");
    workConf.setNewConnectionPerTxn(true);
    return new SeededWorker(new MockBenchmark(workConf), 0);
  }

  private static void seedLatencies(Worker<?> w, int... samplesUs) throws Exception {
    Field f = Worker.class.getDeclaredField("intervalLatenciesMicros");
    f.setAccessible(true);
    @SuppressWarnings("unchecked")
    ConcurrentLinkedQueue<Integer> q = (ConcurrentLinkedQueue<Integer>) f.get(w);
    for (int s : samplesUs) {
      q.offer(s);
    }
  }

  private MonitorInfo buildInfo(MonitoringType type, int intervalMs, Path csvPath) {
    return new MonitorInfo() {
      @Override
      public int getMonitoringInterval() {
        return intervalMs;
      }

      @Override
      public MonitoringType getMonitoringType() {
        return type;
      }

      @Override
      public Optional<Path> getLatencyReportCsvPath() {
        return Optional.ofNullable(csvPath);
      }
    };
  }

  @Test
  public void testDrainLatencySamplesMergesAcrossWorkers() throws Exception {
    SeededWorker w1 = makeWorker();
    SeededWorker w2 = makeWorker();
    seedLatencies(w1, 100, 200);
    seedLatencies(w2, 300);

    TestMonitorImpl m =
        new TestMonitorImpl(
            buildInfo(MonitoringType.LATENCY, 1000, null),
            benchmarkState,
            Arrays.asList(w1, w2));

    int[] merged = m.drainLatencySamples();
    Arrays.sort(merged);
    // All three samples are merged; order across workers isn't guaranteed so we sort.
    assertEquals(3, merged.length);
    assertEquals(100, merged[0]);
    assertEquals(200, merged[1]);
    assertEquals(300, merged[2]);

    // Second drain returns nothing since the buffers were emptied.
    assertEquals(0, m.drainLatencySamples().length);
  }

  @Test
  public void testLatencyCsvHasHeaderAndRows() throws Exception {
    SeededWorker w = makeWorker();

    TestMonitorImpl m =
        new TestMonitorImpl(
            buildInfo(MonitoringType.LATENCY, 500, tempCsv),
            benchmarkState,
            java.util.Collections.singletonList(w));

    m.initForTest();

    // First tick: 4 samples.
    seedLatencies(w, 1000, 2000, 3000, 4000);
    m.tickForTest(500, 8.0d);

    // Second tick: 2 samples (smaller interval).
    seedLatencies(w, 5000, 15000);
    m.tickForTest(500, 4.0d);

    m.tearDown();

    List<String> lines = Files.readAllLines(tempCsv);
    assertEquals(
        "Expected header + 2 data rows in CSV: " + lines, 3, lines.size());
    assertEquals(Monitor.LATENCY_CSV_HEADER, lines.get(0));

    String row1 = lines.get(1);
    String row2 = lines.get(2);

    String[] r1 = row1.split(",");
    // Columns: elapsed_sec,interval_ms,count,tps,min,p25,p50,p75,p90,p95,p99,max,avg,stdev
    assertEquals("row1 col count: " + row1, 14, r1.length);
    assertEquals("500", r1[1]);
    assertEquals("4", r1[2]);
    // min=1000us, max=4000us for first window
    assertEquals("1000", r1[4]);
    assertEquals("4000", r1[11]);

    String[] r2 = row2.split(",");
    assertEquals("2", r2[2]);
    assertEquals("5000", r2[4]);
    assertEquals("15000", r2[11]);
  }

  @Test
  public void testThroughputOnlyModeWritesNoCsv() throws Exception {
    SeededWorker w = makeWorker();
    seedLatencies(w, 1000, 2000);

    TestMonitorImpl m =
        new TestMonitorImpl(
            buildInfo(MonitoringType.THROUGHPUT, 500, tempCsv),
            benchmarkState,
            java.util.Collections.singletonList(w));

    m.initForTest();
    m.tickForTest(500, 1.0d);
    m.tearDown();

    // CSV was created by setUp() but since latency reporting is off, the monitor should not have
    // written any header or rows. The file must still be empty.
    assertTrue(
        "Expected temp CSV to remain empty in THROUGHPUT mode",
        Files.size(tempCsv) == 0);

    // Samples remain in the worker's buffer because latency reporting never drained them.
    int[] stillThere = w.drainIntervalLatenciesMicros();
    assertEquals(2, stillThere.length);
  }

  @Test
  public void testEmptyIntervalStillEmitsRow() throws Exception {
    SeededWorker w = makeWorker();

    TestMonitorImpl m =
        new TestMonitorImpl(
            buildInfo(MonitoringType.LATENCY, 500, tempCsv),
            benchmarkState,
            java.util.Collections.singletonList(w));

    m.initForTest();
    // No samples seeded -> empty distribution. Tick should still produce a row.
    m.tickForTest(500, 0.0d);
    m.tearDown();

    List<String> lines = Files.readAllLines(tempCsv);
    assertEquals(2, lines.size());
    String[] row = lines.get(1).split(",");
    assertEquals("0", row[2]); // count == 0
  }
}
