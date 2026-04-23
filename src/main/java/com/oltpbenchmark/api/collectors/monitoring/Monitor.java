package com.oltpbenchmark.api.collectors.monitoring;

import com.oltpbenchmark.BenchmarkState;
import com.oltpbenchmark.DistributionStatistics;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.util.MonitorInfo;
import com.oltpbenchmark.util.MonitorInfo.MonitoringType;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic monitoring class that reports the throughput of the executing workers while the benchmark
 * is being executed. When {@link MonitoringType#LATENCY} or {@link MonitoringType#ADVANCED} is
 * configured, the monitor additionally aggregates per-interval latency samples into a {@link
 * DistributionStatistics} and (optionally) appends a row to a CSV file.
 */
public class Monitor extends Thread {
  protected static final Logger LOG = LoggerFactory.getLogger(Monitor.class);

  static final String LATENCY_CSV_HEADER =
      "elapsed_sec,interval_ms,count,tps,min_us,p25_us,p50_us,p75_us,p90_us,p95_us,p99_us,max_us,avg_us,stdev_us";

  protected final MonitorInfo monitorInfo;
  protected final BenchmarkState testState;
  protected final List<? extends Worker<? extends BenchmarkModule>> workers;

  private BufferedWriter latencyCsvWriter;
  private long monitorStartNanos;

  {
    this.setDaemon(true);
  }

  /**
   * @param monitorInfo monitoring configuration (interval, type, optional CSV path)
   * @param testState shared benchmark state
   * @param workers workers whose samples should be aggregated
   */
  Monitor(
      MonitorInfo monitorInfo,
      BenchmarkState testState,
      List<? extends Worker<? extends BenchmarkModule>> workers) {
    this.monitorInfo = monitorInfo;
    this.testState = testState;
    this.workers = workers;
  }

  @Override
  public void run() {
    int interval = this.monitorInfo.getMonitoringInterval();

    LOG.info(
        "Starting MonitorThread type={} interval=[{}ms]",
        this.monitorInfo.getMonitoringType(),
        interval);

    initLatencyReportingIfEnabled();

    while (!Thread.currentThread().isInterrupted()) {
      // Compute the last throughput.
      long measuredRequests = 0;
      synchronized (this.testState) {
        for (Worker<?> w : this.workers) {
          measuredRequests += w.getAndResetIntervalRequests();
        }
      }
      double seconds = interval / 1000d;
      double tps = (double) measuredRequests / seconds;
      LOG.info("Throughput: {} txn/sec", tps);

      reportLatencyTick(interval, tps);

      try {
        Thread.sleep(interval);
      } catch (InterruptedException ex) {
        // Restore interrupt flag.
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Open the latency CSV (if configured) and seed the monitor start time. Subclasses that override
   * {@link #run()} can call this in their own setup so latency reporting still works.
   */
  protected final void initLatencyReportingIfEnabled() {
    if (!emitsLatency()) {
      return;
    }
    openLatencyCsvIfConfigured();
    this.monitorStartNanos = System.nanoTime();
  }

  /**
   * Emit a single per-interval latency report (log line + CSV row) when latency reporting is
   * enabled. No-op otherwise. Subclasses that override {@link #run()} (e.g. {@link
   * DatabaseMonitor}) can call this once per tick.
   *
   * @param intervalMs monitoring interval in milliseconds (used in the log line and CSV)
   * @param tps throughput measured for the just-completed interval
   */
  protected final void reportLatencyTick(int intervalMs, double tps) {
    if (!emitsLatency()) {
      return;
    }
    int[] samples = drainLatencySamples();
    DistributionStatistics stats = DistributionStatistics.computeStatistics(samples);
    double elapsedSec = (System.nanoTime() - monitorStartNanos) / 1_000_000_000d;

    LOG.info(
        "Latency [window={}ms] count={} tps={} p50={}us p95={}us p99={}us min={}us max={}us avg={}us",
        intervalMs,
        stats.getCount(),
        String.format(Locale.ROOT, "%.2f", tps),
        (long) stats.getMedian(),
        (long) stats.get95thPercentile(),
        (long) stats.get99thPercentile(),
        (long) stats.getMinimum(),
        (long) stats.getMaximum(),
        String.format(Locale.ROOT, "%.1f", stats.getAverage()));

    writeLatencyCsvRow(elapsedSec, intervalMs, stats, tps);
  }

  /**
   * Drain and merge per-interval latency samples from all workers.
   *
   * <p>Package-private so tests can exercise the aggregation logic directly.
   */
  int[] drainLatencySamples() {
    int total = 0;
    int[][] perWorker = new int[workers.size()][];
    for (int i = 0; i < workers.size(); ++i) {
      int[] drained = workers.get(i).drainIntervalLatenciesMicros();
      perWorker[i] = drained;
      total += drained.length;
    }
    int[] merged = new int[total];
    int offset = 0;
    for (int[] chunk : perWorker) {
      System.arraycopy(chunk, 0, merged, offset, chunk.length);
      offset += chunk.length;
    }
    return merged;
  }

  private boolean emitsLatency() {
    MonitoringType t = this.monitorInfo.getMonitoringType();
    return t == MonitoringType.LATENCY || t == MonitoringType.ADVANCED;
  }

  private void openLatencyCsvIfConfigured() {
    Optional<Path> path = this.monitorInfo.getLatencyReportCsvPath();
    if (path.isEmpty()) {
      return;
    }
    Path csv = path.get();
    try {
      if (csv.getParent() != null) {
        Files.createDirectories(csv.getParent());
      }
      this.latencyCsvWriter =
          Files.newBufferedWriter(
              csv,
              StandardOpenOption.CREATE,
              StandardOpenOption.TRUNCATE_EXISTING,
              StandardOpenOption.WRITE);
      this.latencyCsvWriter.write(LATENCY_CSV_HEADER);
      this.latencyCsvWriter.newLine();
      this.latencyCsvWriter.flush();
      LOG.info("Writing per-interval latency metrics to {}", csv.toAbsolutePath());
    } catch (IOException e) {
      LOG.warn("Failed to open latency CSV at {}: {}", csv, e.getMessage());
      this.latencyCsvWriter = null;
    }
  }

  private void writeLatencyCsvRow(
      double elapsedSec, int intervalMs, DistributionStatistics stats, double tps) {
    if (this.latencyCsvWriter == null) {
      return;
    }
    try {
      this.latencyCsvWriter.write(
          String.format(
              Locale.ROOT,
              "%.3f,%d,%d,%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%.3f,%.3f",
              elapsedSec,
              intervalMs,
              stats.getCount(),
              tps,
              (long) stats.getMinimum(),
              (long) stats.get25thPercentile(),
              (long) stats.getMedian(),
              (long) stats.get75thPercentile(),
              (long) stats.get90thPercentile(),
              (long) stats.get95thPercentile(),
              (long) stats.get99thPercentile(),
              (long) stats.getMaximum(),
              stats.getAverage(),
              stats.getStandardDeviation()));
      this.latencyCsvWriter.newLine();
      this.latencyCsvWriter.flush();
    } catch (IOException e) {
      LOG.warn("Failed to append latency CSV row: {}", e.getMessage());
    }
  }

  /** Called at the end of the test to do any clean up that may be required. */
  public void tearDown() {
    if (this.latencyCsvWriter != null) {
      try {
        this.latencyCsvWriter.flush();
        this.latencyCsvWriter.close();
      } catch (IOException e) {
        LOG.warn("Failed to close latency CSV: {}", e.getMessage());
      } finally {
        this.latencyCsvWriter = null;
      }
    }
  }
}
