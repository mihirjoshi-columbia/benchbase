/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.util.ClassUtil;
import com.oltpbenchmark.util.FileUtil;
import com.oltpbenchmark.util.JSONSerializable;
import com.oltpbenchmark.util.JSONUtil;
import com.oltpbenchmark.util.MonitorInfo;
import com.oltpbenchmark.util.ResultWriter;
import com.oltpbenchmark.util.StringUtil;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DisabledListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBWorkload {
  private static final Logger LOG = LoggerFactory.getLogger(DBWorkload.class);

  private static final String SINGLE_LINE = StringUtil.repeat("=", 70);

  private static final String RATE_DISABLED = "disabled";
  private static final String RATE_UNLIMITED = "unlimited";

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    // create the command line parser
    CommandLineParser parser = new DefaultParser();

    XMLConfiguration pluginConfig = buildConfiguration("config/plugin.xml");

    Options options = buildOptions(pluginConfig);

    CommandLine argsLine = parser.parse(options, args);

    if (argsLine.hasOption("h")) {
      printUsage(options);
      return;
    } else if (!argsLine.hasOption("c")) {
      LOG.error("Missing Configuration file");
      printUsage(options);
      return;
    } else if (!argsLine.hasOption("b")) {
      LOG.error("Missing Benchmark Class to load");
      printUsage(options);
      return;
    }

    // Monitoring setup.
    // Create a simple MonitorInfo implementation since ImmutableMonitorInfo is not available
    class SimpleMonitorInfo implements MonitorInfo {
      private int interval = 0;
      private MonitorInfo.MonitoringType type = MonitorInfo.MonitoringType.THROUGHPUT;
      private java.util.Optional<java.nio.file.Path> latencyCsvPath = java.util.Optional.empty();

      @Override
      public int getMonitoringInterval() {
        return interval;
      }

      @Override
      public MonitoringType getMonitoringType() {
        return type;
      }

      @Override
      public java.util.Optional<java.nio.file.Path> getLatencyReportCsvPath() {
        return latencyCsvPath;
      }

      // Custom setters
      public void updateInterval(int val) {
        this.interval = val;
      }

      public void updateType(MonitoringType val) {
        this.type = val;
      }

      public void updateLatencyCsvPath(java.nio.file.Path p) {
        this.latencyCsvPath = java.util.Optional.ofNullable(p);
      }
    }

    SimpleMonitorInfo monitorInfoImpl = new SimpleMonitorInfo();
    MonitorInfo monitorInfo = monitorInfoImpl;

    if (argsLine.hasOption("im")) {
      monitorInfoImpl.updateInterval(Integer.parseInt(argsLine.getOptionValue("im")));
    }
    if (argsLine.hasOption("mt")) {
      switch (argsLine.getOptionValue("mt")) {
        case "advanced":
          monitorInfoImpl.updateType(MonitorInfo.MonitoringType.ADVANCED);
          break;
        case "throughput":
          monitorInfoImpl.updateType(MonitorInfo.MonitoringType.THROUGHPUT);
          break;
        case "latency":
          monitorInfoImpl.updateType(MonitorInfo.MonitoringType.LATENCY);
          break;
        default:
          throw new ParseException(
              "Monitoring type '"
                  + argsLine.getOptionValue("mt")
                  + "' is undefined, allowed values are: advanced/throughput/latency");
      }
    }

    // When latency reporting is requested, derive a CSV path for per-interval metrics from -d.
    if (monitorInfoImpl.getMonitoringType() == MonitorInfo.MonitoringType.LATENCY
        || monitorInfoImpl.getMonitoringType() == MonitorInfo.MonitoringType.ADVANCED) {
      if (monitorInfoImpl.getMonitoringInterval() > 0) {
        String baseDir = argsLine.getOptionValue("d", "results");
        String bench = argsLine.getOptionValue("b", "benchmark");
        String name =
            org.apache.commons.lang3.StringUtils.join(
                org.apache.commons.lang3.StringUtils.split(bench, ','), '-');
        String fileName =
            name + "_" + com.oltpbenchmark.util.TimeUtil.getCurrentTimeString()
                + ".latency-intervals.csv";
        java.nio.file.Path csvPath = java.nio.file.Paths.get(baseDir, fileName);
        monitorInfoImpl.updateLatencyCsvPath(csvPath);
        LOG.info("Per-interval latency metrics will be written to {}", csvPath.toAbsolutePath());
      } else {
        LOG.warn(
            "Latency monitoring requested (-mt {}) but no interval was set via -im/--interval-monitor; no live reports will be emitted.",
            monitorInfoImpl.getMonitoringType());
      }
    }

    // -------------------------------------------------------------------
    // GET PLUGIN LIST
    // -------------------------------------------------------------------

    String targetBenchmarks = argsLine.getOptionValue("b");

    // -------------------------------------------------------------------
    // CHECK FOR CONTINUOUS REPORTING MODE
    // -------------------------------------------------------------------

    boolean continuousReporting = false;
    int continuousWindow = 15; // Default window size
    boolean continuousPerf = false;
    int continuousBuffer = 0; // Default no buffer between windows
    if (argsLine.hasOption("continuous")) {
      continuousReporting = true;
      if (argsLine.hasOption("continuous-window")) {
        continuousWindow = Integer.parseInt(argsLine.getOptionValue("continuous-window"));
      }
      if (argsLine.hasOption("continuous-perf")) {
        continuousPerf = true;
      }
      if (argsLine.hasOption("continuous-buffer")) {
        continuousBuffer = Integer.parseInt(argsLine.getOptionValue("continuous-buffer"));
      }
      LOG.info(SINGLE_LINE);
      LOG.info(
          "Continuous reporting mode enabled with window size of {} seconds", continuousWindow);
      if (continuousBuffer > 0) {
        LOG.info("Buffer time between windows: {} seconds", continuousBuffer);
      }
      LOG.info("Live window metrics will be reported periodically during the benchmark run");
      LOG.info("Each window captures only transactions executed during that specific time period");
      LOG.info(
          "Latency samples are collected based on the number of successful transactions in each window");
      LOG.info("Window reports will be saved to the results directory");
      if (continuousPerf) {
        LOG.info("Performance measurements will be collected during continuous reporting");
      }
      LOG.info(
          "TIP: Consider using --barebones-run with --continuous for best monitoring experience");
      LOG.info(
          "Note: This mode runs alongside normal benchmark execution and reports live metrics");
      LOG.info(
          "      with latency distribution (Avg, P25, P50, P75, P90, P99, Min, Max) and goodput");
      LOG.info(SINGLE_LINE);
    }

    String[] targetList = targetBenchmarks.split(",");
    List<BenchmarkModule> benchList = new ArrayList<>();

    // Use this list for filtering of the output
    List<TransactionType> activeTXTypes = new ArrayList<>();

    String configFile = argsLine.getOptionValue("c");

    XMLConfiguration xmlConfig = buildConfiguration(configFile);

    // Load the configuration for each benchmark
    int lastTxnId = 0;
    for (String plugin : targetList) {
      String pluginTest = "[@bench='" + plugin + "']";

      // ----------------------------------------------------------------
      // BEGIN LOADING WORKLOAD CONFIGURATION
      // ----------------------------------------------------------------

      WorkloadConfiguration wrkld = new WorkloadConfiguration();
      wrkld.setBenchmarkName(plugin);
      wrkld.setXmlConfig(xmlConfig);

      // Pull in database configuration
      wrkld.setDatabaseType(DatabaseType.get(xmlConfig.getString("type")));
      wrkld.setDriverClass(xmlConfig.getString("driver"));
      wrkld.setUrl(xmlConfig.getString("url"));
      wrkld.setUsername(xmlConfig.getString("username"));
      wrkld.setPassword(xmlConfig.getString("password"));
      wrkld.setRandomSeed(xmlConfig.getInt("randomSeed", -1));
      wrkld.setBatchSize(xmlConfig.getInt("batchsize", 128));
      wrkld.setMaxRetries(xmlConfig.getInt("retries", 3));
      wrkld.setNewConnectionPerTxn(xmlConfig.getBoolean("newConnectionPerTxn", false));
      wrkld.setReconnectOnConnectionFailure(
          xmlConfig.getBoolean("reconnectOnConnectionFailure", false));

      int terminals = xmlConfig.getInt("terminals[not(@bench)]", 0);
      terminals = xmlConfig.getInt("terminals" + pluginTest, terminals);
      wrkld.setTerminals(terminals);

      if (xmlConfig.containsKey("loaderThreads")) {
        int loaderThreads = xmlConfig.getInt("loaderThreads");
        wrkld.setLoaderThreads(loaderThreads);
      }

      String isolationMode =
          xmlConfig.getString("isolation[not(@bench)]", "TRANSACTION_SERIALIZABLE");
      wrkld.setIsolationMode(xmlConfig.getString("isolation" + pluginTest, isolationMode));
      wrkld.setScaleFactor(xmlConfig.getDouble("scalefactor", 1.0));
      wrkld.setDataDir(xmlConfig.getString("datadir", "."));
      wrkld.setDDLPath(xmlConfig.getString("ddlpath", null));

      double selectivity = -1;
      try {
        selectivity = xmlConfig.getDouble("selectivity");
        wrkld.setSelectivity(selectivity);
      } catch (NoSuchElementException nse) {
        // Nothing to do here !
      }

      // Set monitoring enabled, if all requirements are met.
      if (monitorInfo.getMonitoringInterval() > 0
          && monitorInfo.getMonitoringType() == MonitorInfo.MonitoringType.ADVANCED
          && DatabaseType.get(xmlConfig.getString("type")).shouldCreateMonitoringPrefix()) {
        LOG.info("Advanced monitoring enabled, prefix will be added to queries.");
        wrkld.setAdvancedMonitoringEnabled(true);
      }

      // ----------------------------------------------------------------
      // CREATE BENCHMARK MODULE
      // ----------------------------------------------------------------

      String classname = pluginConfig.getString("/plugin[@name='" + plugin + "']");

      if (classname == null) {
        throw new ParseException("Plugin " + plugin + " is undefined in config/plugin.xml");
      }

      BenchmarkModule bench =
          ClassUtil.newInstance(
              classname, new Object[] {wrkld}, new Class<?>[] {WorkloadConfiguration.class});
      Map<String, Object> initDebug = new ListOrderedMap<>();
      initDebug.put("Benchmark", String.format("%s {%s}", plugin.toUpperCase(), classname));
      initDebug.put("Configuration", configFile);
      initDebug.put("Type", wrkld.getDatabaseType());
      initDebug.put("Driver", wrkld.getDriverClass());
      initDebug.put("URL", wrkld.getUrl());
      initDebug.put("Isolation", wrkld.getIsolationString());
      initDebug.put("Batch Size", wrkld.getBatchSize());
      initDebug.put("Scale Factor", wrkld.getScaleFactor());
      initDebug.put("Terminals", wrkld.getTerminals());
      initDebug.put("New Connection Per Txn", wrkld.getNewConnectionPerTxn());
      initDebug.put("Reconnect on Connection Failure", wrkld.getReconnectOnConnectionFailure());

      if (selectivity != -1) {
        initDebug.put("Selectivity", selectivity);
      }

      // Add scheduler parameters to debug output if available
      if (argsLine.hasOption("sp")) {
        String schedulerParamsStr = argsLine.getOptionValue("sp");
        initDebug.put("Scheduler Parameters", schedulerParamsStr);
      }

      // Add measurement window length to debug output if available
      if (argsLine.hasOption("mw")) {
        int measurementWindowSeconds = Integer.parseInt(argsLine.getOptionValue("mw"));
        initDebug.put("Measurement Window (seconds)", measurementWindowSeconds);
      } else {
        initDebug.put("Measurement Window (seconds)", "15 (default)");
      }

      LOG.info("{}\n\n{}", SINGLE_LINE, StringUtil.formatMaps(initDebug));
      LOG.info(SINGLE_LINE);

      // ----------------------------------------------------------------
      // LOAD TRANSACTION DESCRIPTIONS
      // ----------------------------------------------------------------
      int numTxnTypes =
          xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
      if (numTxnTypes == 0 && targetList.length == 1) {
        // if it is a single workload run, <transactiontypes /> w/o attribute is used
        pluginTest = "[not(@bench)]";
        numTxnTypes =
            xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
      }

      List<TransactionType> ttypes = new ArrayList<>();
      ttypes.add(TransactionType.INVALID);
      int txnIdOffset = lastTxnId;
      for (int i = 1; i <= numTxnTypes; i++) {
        String key = "transactiontypes" + pluginTest + "/transactiontype[" + i + "]";
        String txnName = xmlConfig.getString(key + "/name");

        // Get ID if specified; else increment from last one.
        int txnId = i;
        if (xmlConfig.containsKey(key + "/id")) {
          txnId = xmlConfig.getInt(key + "/id");
        }

        long preExecutionWait = 0;
        if (xmlConfig.containsKey(key + "/preExecutionWait")) {
          preExecutionWait = xmlConfig.getLong(key + "/preExecutionWait");
        }

        long postExecutionWait = 0;
        if (xmlConfig.containsKey(key + "/postExecutionWait")) {
          postExecutionWait = xmlConfig.getLong(key + "/postExecutionWait");
        }

        // After load
        if (xmlConfig.containsKey("afterload")) {
          bench.setAfterLoadScriptPath(xmlConfig.getString("afterload"));
        }

        TransactionType tmpType =
            bench.initTransactionType(
                txnName, txnId + txnIdOffset, preExecutionWait, postExecutionWait);

        // Keep a reference for filtering
        activeTXTypes.add(tmpType);

        // Add a ref for the active TTypes in this benchmark
        ttypes.add(tmpType);
        lastTxnId = i;
      }

      // Wrap the list of transactions and save them
      TransactionTypes tt = new TransactionTypes(ttypes);
      wrkld.setTransTypes(tt);
      LOG.debug("Using the following transaction types: {}", tt);

      // Read in the groupings of transactions (if any) defined for this
      // benchmark
      int numGroupings =
          xmlConfig
              .configurationsAt("transactiontypes" + pluginTest + "/groupings/grouping")
              .size();
      LOG.debug("Num groupings: {}", numGroupings);
      for (int i = 1; i < numGroupings + 1; i++) {
        String key = "transactiontypes" + pluginTest + "/groupings/grouping[" + i + "]";

        // Get the name for the grouping and make sure it's valid.
        String groupingName = xmlConfig.getString(key + "/name").toLowerCase();
        if (!groupingName.matches("^[a-z]\\w*$")) {
          LOG.error(
              String.format(
                  "Grouping name \"%s\" is invalid."
                      + " Must begin with a letter and contain only"
                      + " alphanumeric characters.",
                  groupingName));
          System.exit(-1);
        } else if (groupingName.equals("all")) {
          LOG.error("Grouping name \"all\" is reserved." + " Please pick a different name.");
          System.exit(-1);
        }

        // Get the weights for this grouping and make sure that there
        // is an appropriate number of them.
        List<String> groupingWeights =
            Arrays.asList(xmlConfig.getString(key + "/weights").split("\\s*,\\s*"));
        if (groupingWeights.size() != numTxnTypes) {
          LOG.error(
              String.format(
                  "Grouping \"%s\" has %d weights,"
                      + " but there are %d transactions in this"
                      + " benchmark.",
                  groupingName, groupingWeights.size(), numTxnTypes));
          System.exit(-1);
        }

        LOG.debug("Creating grouping with name, weights: {}, {}", groupingName, groupingWeights);
      }

      benchList.add(bench);

      // ----------------------------------------------------------------
      // WORKLOAD CONFIGURATION
      // ----------------------------------------------------------------

      int size = xmlConfig.configurationsAt("/works/work").size();
      for (int i = 1; i < size + 1; i++) {
        final HierarchicalConfiguration<ImmutableNode> work =
            xmlConfig.configurationAt("works/work[" + i + "]");
        List<String> weight_strings;

        // use a workaround if there are multiple workloads or single
        // attributed workload
        if (targetList.length > 1 || work.containsKey("weights[@bench]")) {
          weight_strings = Arrays.asList(work.getString("weights" + pluginTest).split("\\s*,\\s*"));
        } else {
          weight_strings = Arrays.asList(work.getString("weights[not(@bench)]").split("\\s*,\\s*"));
        }

        double rate = 1;
        boolean rateLimited = true;
        boolean disabled = false;
        boolean timed;

        // can be "disabled", "unlimited" or a number
        String rate_string;
        rate_string = work.getString("rate[not(@bench)]", "");
        rate_string = work.getString("rate" + pluginTest, rate_string);
        if (rate_string.equals(RATE_DISABLED)) {
          disabled = true;
        } else if (rate_string.equals(RATE_UNLIMITED)) {
          rateLimited = false;
        } else if (rate_string.isEmpty()) {
          LOG.error(
              String.format("Please specify the rate for phase %d and workload %s", i, plugin));
          System.exit(-1);
        } else {
          try {
            rate = Double.parseDouble(rate_string);
            if (rate <= 0) {
              LOG.error("Rate limit must be at least 0. Use unlimited or disabled values instead.");
              System.exit(-1);
            }
          } catch (NumberFormatException e) {
            LOG.error(
                String.format(
                    "Rate string must be '%s', '%s' or a number", RATE_DISABLED, RATE_UNLIMITED));
            System.exit(-1);
          }
        }
        Phase.Arrival arrival = Phase.Arrival.REGULAR;
        String arrive = work.getString("@arrival", "regular");
        if (arrive.equalsIgnoreCase("POISSON")) {
          arrival = Phase.Arrival.POISSON;
        }

        // We now have the option to run all queries exactly once in
        // a serial (rather than random) order.
        boolean serial = Boolean.parseBoolean(work.getString("serial", Boolean.FALSE.toString()));

        int activeTerminals;
        activeTerminals = work.getInt("active_terminals[not(@bench)]", terminals);
        activeTerminals = work.getInt("active_terminals" + pluginTest, activeTerminals);
        // If using serial, we should have only one terminal
        if (serial && activeTerminals != 1) {
          LOG.warn("Serial ordering is enabled, so # of active terminals is clamped to 1.");
          activeTerminals = 1;
        }
        if (activeTerminals > terminals) {
          LOG.error(
              String.format(
                  "Configuration error in work %d: "
                      + "Number of active terminals is bigger than the total number of terminals",
                  i));
          System.exit(-1);
        }

        int time = work.getInt("/time", 0);
        int warmup = work.getInt("/warmup", 0);
        timed = (time > 0);
        if (!timed) {
          if (serial) {
            LOG.info("Timer disabled for serial run; will execute" + " all queries exactly once.");
          } else {
            LOG.error(
                "Must provide positive time bound for"
                    + " non-serial executions. Either provide"
                    + " a valid time or enable serial mode.");
            System.exit(-1);
          }
        } else if (serial) {
          LOG.info(
              "Timer enabled for serial run; will run queries"
                  + " serially in a loop until the timer expires.");
        }
        if (warmup < 0) {
          LOG.error("Must provide non-negative time bound for" + " warmup.");
          System.exit(-1);
        }

        ArrayList<Double> weights = new ArrayList<>();

        double totalWeight = 0;

        for (String weightString : weight_strings) {
          double weight = Double.parseDouble(weightString);
          totalWeight += weight;
          weights.add(weight);
        }

        long roundedWeight = Math.round(totalWeight);

        if (roundedWeight != 100) {
          LOG.warn(
              "rounded weight [{}] does not equal 100.  Original weight is [{}]",
              roundedWeight,
              totalWeight);
        }

        wrkld.addPhase(
            i,
            time,
            warmup,
            rate,
            weights,
            rateLimited,
            disabled,
            serial,
            timed,
            activeTerminals,
            arrival);
      }

      // CHECKING INPUT PHASES
      int j = 0;
      for (Phase p : wrkld.getPhases()) {
        j++;
        if (p.getWeightCount() != numTxnTypes) {
          LOG.error(
              String.format(
                  "Configuration files is inconsistent, phase %d contains %d weights but you defined %d transaction types",
                  j, p.getWeightCount(), numTxnTypes));
          if (p.isSerial()) {
            LOG.error(
                "However, note that since this a serial phase, the weights are irrelevant (but still must be included---sorry).");
          }
          System.exit(-1);
        }
      }

      // Generate the dialect map
      wrkld.init();
    }

    // Export StatementDialects
    if (isBooleanOptionSet(argsLine, "dialects-export")) {
      BenchmarkModule bench = benchList.get(0);
      if (bench.getStatementDialects() != null) {
        LOG.info("Exporting StatementDialects for {}", bench);
        String xml =
            bench
                .getStatementDialects()
                .export(
                    bench.getWorkloadConfiguration().getDatabaseType(),
                    bench.getProcedures().values());
        LOG.debug(xml);
        System.exit(0);
      }
      throw new RuntimeException("No StatementDialects is available for " + bench);
    }

    // Create the Benchmark's Database
    if (isBooleanOptionSet(argsLine, "create")) {
      try {
        for (BenchmarkModule benchmark : benchList) {
          LOG.info("Creating new {} database...", benchmark.getBenchmarkName().toUpperCase());
          runCreator(benchmark);
          LOG.info(
              "Finished creating new {} database...", benchmark.getBenchmarkName().toUpperCase());
        }
      } catch (Throwable ex) {
        LOG.error("Unexpected error when creating benchmark database tables.", ex);
        System.exit(1);
      }
    } else {
      LOG.debug("Skipping creating benchmark database tables");
    }

    // Refresh the catalog.
    for (BenchmarkModule benchmark : benchList) {
      benchmark.refreshCatalog();
    }

    // Clear the Benchmark's Database
    if (isBooleanOptionSet(argsLine, "clear")) {
      try {
        for (BenchmarkModule benchmark : benchList) {
          LOG.info("Clearing {} database...", benchmark.getBenchmarkName().toUpperCase());
          benchmark.refreshCatalog();
          benchmark.clearDatabase();
          benchmark.refreshCatalog();
          LOG.info("Finished clearing {} database...", benchmark.getBenchmarkName().toUpperCase());
        }
      } catch (Throwable ex) {
        LOG.error("Unexpected error when clearing benchmark database tables.", ex);
        System.exit(1);
      }
    } else {
      LOG.debug("Skipping clearing benchmark database tables");
    }

    // Execute Loader
    if (isBooleanOptionSet(argsLine, "load")) {
      try {
        for (BenchmarkModule benchmark : benchList) {
          LOG.info("Loading data into {} database...", benchmark.getBenchmarkName().toUpperCase());
          runLoader(benchmark);
          LOG.info(
              "Finished loading data into {} database...",
              benchmark.getBenchmarkName().toUpperCase());
        }
      } catch (Throwable ex) {
        LOG.error("Unexpected error when loading benchmark database records.", ex);
        System.exit(1);
      }

    } else {
      LOG.debug("Skipping loading benchmark database records");
    }

    // Anonymize Datasets
    // Currently, the system only parses the config but does not run any anonymization!
    // Will be added in the future
    if (isBooleanOptionSet(argsLine, "anonymize")) {
      try {
        if (xmlConfig.configurationsAt("/anonymization/table").size() > 0) {
          applyAnonymization(xmlConfig, configFile);
        }
      } catch (Throwable ex) {
        LOG.error("Unexpected error when anonymizing datasets", ex);
        System.exit(1);
      }
    }

    // Execute Workload
    if (isBooleanOptionSet(argsLine, "execute")) {
      // Bombs away!
      try {
        // If an output directory is used, store the information
        String outputDirectory = "results";
        if (argsLine.hasOption("d")) {
          outputDirectory = argsLine.getOptionValue("d");
        }

        // Print paths for easy reference
        File outputDirFile = new File(outputDirectory);
        String absOutputPath = outputDirFile.getAbsolutePath();
        LOG.info("{}", SINGLE_LINE);
        LOG.info("Benchmark result files will be stored in: {}", absOutputPath);
        LOG.info("Performance measurement files will be stored in: {}/perf", absOutputPath);
        LOG.info("{}", SINGLE_LINE);

        // Check for barebones mode
        boolean bareBonesRun = argsLine.hasOption("barebones-run");
        if (bareBonesRun) {
          LOG.info("Barebones run detected: disabling performance measurements");
          Results.setBarebonesMode(true);
        }

        // Print out continuous reporting mode information if enabled
        if (continuousReporting) {
          LOG.info("Enabling continuous reporting mode:");
          LOG.info("  - Window Size: {}s", continuousWindow);
          LOG.info(
              "  - Measurement windows will capture only transactions executed during that specific time period");
          LOG.info(
              "  - Latency samples are collected based on the number of successful transactions in each window");
          LOG.info(
              "  - Live window metrics will be reported periodically during the benchmark run");
          LOG.info(
              "  - Performance metrics will be collected via 'perf' for the entire window duration when --continuous-perf is enabled");

          Results.setContinuousReportingMode(true, continuousWindow, continuousBuffer);
        }

        Results r =
            runWorkload(
                benchList,
                monitorInfo,
                argsLine,
                continuousReporting,
                continuousWindow,
                continuousPerf,
                continuousBuffer);
        writeOutputs(r, activeTXTypes, argsLine, xmlConfig, r.baseFileName);
        writeHistograms(r);

        if (argsLine.hasOption("json-histograms")) {
          String histogram_json = writeJSONHistograms(r);
          String fileName = argsLine.getOptionValue("json-histograms");
          FileUtil.writeStringToFile(new File(fileName), histogram_json);
          LOG.info("Histograms JSON Data: " + fileName);
        }

        if (r.getState() == State.ERROR) {
          throw new RuntimeException(
              "Errors encountered during benchmark execution. See output above for details.");
        }
      } catch (Throwable ex) {
        LOG.error("Unexpected error when executing benchmarks.", ex);
        System.exit(1);
      }

    } else {
      LOG.info("Skipping benchmark workload execution");
    }
  }

  private static Options buildOptions(XMLConfiguration pluginConfig) {
    Options options = new Options();
    options.addOption(
        "b",
        "bench",
        true,
        "[required] Benchmark class. Currently supported: "
            + pluginConfig.getList("/plugin//@name"));
    options.addOption("c", "config", true, "[required] Workload configuration file");
    options.addOption(null, "create", true, "Initialize the database for this benchmark");
    options.addOption(null, "clear", true, "Clear all records in the database for this benchmark");
    options.addOption(null, "load", true, "Load data using the benchmark's data loader");
    options.addOption(
        null, "anonymize", true, "Anonymize specified datasets using differential privacy");
    options.addOption(null, "execute", true, "Execute the benchmark workload");
    options.addOption("h", "help", false, "Print this help");
    options.addOption("s", "sample", true, "Sampling window");
    options.addOption("im", "interval-monitor", true, "Monitoring Interval in milliseconds");
    options.addOption(
        "mt",
        "monitor-type",
        true,
        "Type of Monitoring (throughput/advanced/latency). "
            + "'latency' emits per-interval latency percentiles to the log and to "
            + "<dir>/<bench>_<timestamp>.latency-intervals.csv. "
            + "'advanced' does the same plus DB-specific metrics where supported.");
    options.addOption(
        "d",
        "directory",
        true,
        "Base directory for the result files, default is current directory");
    options.addOption(null, "dialects-export", true, "Export benchmark SQL to a dialects file");
    options.addOption("jh", "json-histograms", true, "Export histograms to JSON file");
    options.addOption(
        "sp",
        "scheduler-params",
        true,
        "Scheduler parameters to test in format: param1:val1,val2;param2:val3,val4");
    options.addOption(
        "mw",
        "measurement-window-seconds",
        true,
        "Length of the measurement window for performance tests in seconds (default: 15)");
    options.addOption(
        null,
        "barebones-run",
        false,
        "Run benchmark without scheduler metrics, perf monitoring, or window measurements");
    options.addOption(
        null,
        "continuous",
        false,
        "Enable continuous reporting mode to periodically report benchmark statistics");
    options.addOption(
        null,
        "continuous-window",
        true,
        "Window size in seconds for continuous reporting mode (default: 15)");
    options.addOption(
        null,
        "continuous-perf",
        false,
        "Enable performance measurements during continuous reporting mode");
    options.addOption(
        null,
        "continuous-buffer",
        true,
        "Buffer time in seconds between continuous reporting windows (default: 0)");
    return options;
  }

  public static XMLConfiguration buildConfiguration(String filename) throws ConfigurationException {
    Parameters params = new Parameters();
    FileBasedConfigurationBuilder<XMLConfiguration> builder =
        new FileBasedConfigurationBuilder<>(XMLConfiguration.class)
            .configure(
                params
                    .xml()
                    .setFileName(filename)
                    .setListDelimiterHandler(new DisabledListDelimiterHandler())
                    .setExpressionEngine(new XPathExpressionEngine()));
    return builder.getConfiguration();
  }

  private static void writeHistograms(Results r) {
    StringBuilder sb = new StringBuilder();
    sb.append("\n");

    sb.append(StringUtil.bold("Completed Transactions:"))
        .append("\n")
        .append(r.getSuccess())
        .append("\n\n");

    sb.append(StringUtil.bold("Aborted Transactions:"))
        .append("\n")
        .append(r.getAbort())
        .append("\n\n");

    sb.append(StringUtil.bold("Rejected Transactions (Server Retry):"))
        .append("\n")
        .append(r.getRetry())
        .append("\n\n");

    sb.append(StringUtil.bold("Rejected Transactions (Retry Different):"))
        .append("\n")
        .append(r.getRetryDifferent())
        .append("\n\n");

    sb.append(StringUtil.bold("Unexpected SQL Errors:"))
        .append("\n")
        .append(r.getError())
        .append("\n\n");

    sb.append(StringUtil.bold("Unknown Status Transactions:"))
        .append("\n")
        .append(r.getUnknown())
        .append("\n\n");

    if (!r.getAbortMessages().isEmpty()) {
      sb.append("\n\n")
          .append(StringUtil.bold("User Aborts:"))
          .append("\n")
          .append(r.getAbortMessages());
    }

    LOG.info(SINGLE_LINE);
    LOG.info("Workload Histograms:\n{}", sb);
    LOG.info(SINGLE_LINE);
  }

  private static String writeJSONHistograms(Results r) {
    Map<String, JSONSerializable> map = new HashMap<>();
    map.put("completed", r.getSuccess());
    map.put("aborted", r.getAbort());
    map.put("rejected", r.getRetry());
    map.put("unexpected", r.getError());
    return JSONUtil.toJSONString(map);
  }

  /**
   * Write out the results for a benchmark run to a bunch of files
   *
   * @param r
   * @param activeTXTypes
   * @param argsLine
   * @param xmlConfig
   * @throws Exception
   */
  private static void writeOutputs(
      Results r,
      List<TransactionType> activeTXTypes,
      CommandLine argsLine,
      XMLConfiguration xmlConfig,
      String baseFileName)
      throws Exception {

    // If an output directory is used, store the information
    String outputDirectory = "results";

    if (argsLine.hasOption("d")) {
      outputDirectory = argsLine.getOptionValue("d");
    }

    FileUtil.makeDirIfNotExists(outputDirectory);
    ResultWriter rw = new ResultWriter(r, xmlConfig, argsLine);

    int windowSize = Integer.parseInt(argsLine.getOptionValue("s", "5"));

    String rawFileName = baseFileName + ".raw.csv";
    try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, rawFileName))) {
      LOG.info("Output Raw data into file: {}", rawFileName);
      rw.writeRaw(activeTXTypes, ps);
    }

    String sampleFileName = baseFileName + ".samples.csv";
    try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, sampleFileName))) {
      LOG.info("Output samples into file: {}", sampleFileName);
      rw.writeSamples(ps);
    }

    String summaryFileName = baseFileName + ".summary.json";
    try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, summaryFileName))) {
      LOG.info("Output summary data into file: {}", summaryFileName);
      rw.writeSummary(ps);
    }

    String paramsFileName = baseFileName + ".params.json";
    try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, paramsFileName))) {
      LOG.info("Output DBMS parameters into file: {}", paramsFileName);
      rw.writeParams(ps);
    }

    if (rw.hasMetrics()) {
      String metricsFileName = baseFileName + ".metrics.json";
      try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, metricsFileName))) {
        LOG.info("Output DBMS metrics into file: {}", metricsFileName);
        rw.writeMetrics(ps);
      }
    }

    String configFileName = baseFileName + ".config.xml";
    try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, configFileName))) {
      LOG.info("Output benchmark config into file: {}", configFileName);
      rw.writeConfig(ps);
    }

    String resultsFileName = baseFileName + ".results.csv";
    try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, resultsFileName))) {
      LOG.info("Output results into file: {} with window size {}", resultsFileName, windowSize);
      rw.writeResults(windowSize, ps);
    }

    for (TransactionType t : activeTXTypes) {
      String fileName = baseFileName + ".results." + t.getName() + ".csv";
      try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, fileName))) {
        rw.writeResults(windowSize, ps, t);
      }
    }

    // Generate histograms for each parameter measurement window
    if (!argsLine.hasOption("barebones-run")) {
      rw.writeParameterWindowHistograms(activeTXTypes, outputDirectory, baseFileName);
    } else {
      LOG.info("Barebones run: skipping parameter window histograms");
    }
  }

  private static void runCreator(BenchmarkModule bench) throws SQLException, IOException {
    LOG.debug(String.format("Creating %s Database", bench));
    bench.createDatabase();
  }

  private static void runLoader(BenchmarkModule bench)
      throws IOException, SQLException, InterruptedException {
    LOG.debug(String.format("Loading %s Database", bench));
    bench.loadDatabase();
  }

  private static Results runWorkload(
      List<BenchmarkModule> benchList,
      MonitorInfo monitorInfo,
      CommandLine argsLine,
      boolean continuousReporting,
      int continuousWindow,
      boolean continuousPerf,
      int continuousBuffer)
      throws IOException {
    List<Worker<?>> workers = new ArrayList<>();
    List<WorkloadConfiguration> workConfs = new ArrayList<>();
    for (BenchmarkModule bench : benchList) {
      LOG.info("Creating {} virtual terminals...", bench.getWorkloadConfiguration().getTerminals());
      workers.addAll(bench.makeWorkers());

      int num_phases = bench.getWorkloadConfiguration().getNumberOfPhases();
      LOG.info(
          String.format(
              "Launching the %s Benchmark with %s Phase%s...",
              bench.getBenchmarkName().toUpperCase(), num_phases, (num_phases > 1 ? "s" : "")));
      workConfs.add(bench.getWorkloadConfiguration());
    }

    int totalBenchmarkSeconds = 0;

    // Configure scheduler parameters if provided
    if (argsLine.hasOption("sp")) {
      // Skip scheduler parameter testing if in barebones mode
      if (argsLine.hasOption("barebones-run")) {
        LOG.info("Barebones run: skipping scheduler parameter testing");
      } else {
        String schedulerParamsStr = argsLine.getOptionValue("sp");
        Map<String, List<Object>> schedulerParams = parseSchedulerParams(schedulerParamsStr);

        // Calculate total number of workers across all benchmarks
        int totalWorkers = 0;
        for (WorkloadConfiguration workConf : workConfs) {
          totalWorkers += workConf.getTerminals();
        }

        // Configure the scheduler parameters with the total worker count
        Results.configureSchedulerParams(schedulerParams, totalWorkers);

        // Check if benchmark runtime is sufficient for all parameter combinations
        // Calculate the total runtime across all phases

        for (WorkloadConfiguration workConf : workConfs) {
          for (Phase phase : workConf.getPhases()) {
            // Add both warmup and measured time
            totalBenchmarkSeconds += phase.getTime();
            totalBenchmarkSeconds += phase.getWarmupTime();
            totalBenchmarkSeconds += 2;
          }
        }
      }
    }

    // Configure measurement window length if provided
    if (argsLine.hasOption("mw")) {
      int measurementWindowSeconds = Integer.parseInt(argsLine.getOptionValue("mw"));
      Results.configureMeasurementWindowSeconds(measurementWindowSeconds);
    }

    // Use the default value if the time configuration is insufficient
    String timeConfigWarning = Results.getTimeConfigurationWarning(totalBenchmarkSeconds);

    // Display warning if needed
    if (timeConfigWarning != null) {
      LOG.warn(timeConfigWarning);
    }

    Results r =
        ThreadBench.runRateLimitedBenchmark(
            workers,
            workConfs,
            monitorInfo,
            argsLine,
            continuousReporting,
            continuousWindow,
            continuousPerf,
            continuousBuffer);
    LOG.info(SINGLE_LINE);
    LOG.info("Rate limited reqs/s: {}", r);
    return r;
  }

  /**
   * Parse the scheduler parameters from command line format: param1:val1,val2;param2:val3,val4
   *
   * @param paramStr String in the specified format
   * @return Map from parameter names to lists of values to test
   */
  private static Map<String, List<Object>> parseSchedulerParams(String paramStr) {
    Map<String, List<Object>> result = new HashMap<>();

    // Split parameters by semicolon
    String[] paramPairs = paramStr.split(";");
    for (String paramPair : paramPairs) {
      // Split each parameter into name and values
      String[] parts = paramPair.split(":");
      if (parts.length != 2) {
        LOG.warn("Invalid parameter format: " + paramPair + ". Expected 'name:val1,val2'");
        continue;
      }

      String paramName = parts[0].trim();
      String[] valueStrs = parts[1].split(",");
      List<Object> values = new ArrayList<>();

      // Parse each value (try as long first, then as double if that fails)
      for (String valueStr : valueStrs) {
        valueStr = valueStr.trim();
        try {
          // Try parsing as long first
          values.add(Long.parseLong(valueStr));
        } catch (NumberFormatException e) {
          try {
            // Try parsing as double
            values.add(Double.parseDouble(valueStr));
          } catch (NumberFormatException e2) {
            // Just use as string
            values.add(valueStr);
          }
        }
      }

      result.put(paramName, values);
    }

    return result;
  }

  private static void printUsage(Options options) {
    HelpFormatter hlpfrmt = new HelpFormatter();
    hlpfrmt.printHelp("benchbase", options);
  }

  /**
   * Returns true if the given key is in the CommandLine object and is set to true.
   *
   * @param argsLine
   * @param key
   * @return
   */
  private static boolean isBooleanOptionSet(CommandLine argsLine, String key) {
    if (argsLine.hasOption(key)) {
      LOG.debug("CommandLine has option '{}'. Checking whether set to true", key);
      String val = argsLine.getOptionValue(key);
      LOG.debug(String.format("CommandLine %s => %s", key, val));
      return (val != null && val.equalsIgnoreCase("true"));
    }
    return (false);
  }

  /**
   * Handles the anonymization of specified tables with differential privacy and automatically
   * creates an anonymized copy of the table. Adapts templated query file if sensitive values are
   * present
   *
   * @param xmlConfig
   * @param configFile
   */
  private static void applyAnonymization(XMLConfiguration xmlConfig, String configFile) {

    String templatesPath = "";
    if (xmlConfig.containsKey("query_templates_file")) {
      templatesPath = xmlConfig.getString("query_templates_file");
    }

    LOG.info("Starting the Anonymization process");
    LOG.info(SINGLE_LINE);
    String osCommand = System.getProperty("os.name").startsWith("Windows") ? "python" : "python3";
    ProcessBuilder processBuilder =
        new ProcessBuilder(
            osCommand, "scripts/anonymization/src/anonymizer.py", configFile, templatesPath);
    try {
      // Redirect Output stream of the script to get live feedback
      processBuilder.inheritIO();
      Process process = processBuilder.start();
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        throw new Exception("Anonymization program exited with a non-zero status code");
      }
      LOG.info("Finished the Anonymization process for all tables");
      LOG.info(SINGLE_LINE);
    } catch (Exception e) {
      LOG.error(e.getMessage());
      return;
    }
  }
}
