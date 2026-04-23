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

package com.oltpbenchmark.api;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.types.TransactionStatus;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link Worker#drainIntervalLatenciesMicros()}.
 *
 * <p>Uses a minimal {@link MockBenchmark} so the abstract Worker can be constructed without a live
 * database connection, and reflection to seed the private per-interval latency queue.
 */
public class TestWorkerLatencyBuffer {

  private static final class TestWorker extends Worker<MockBenchmark> {
    TestWorker(MockBenchmark benchmark, int id) {
      super(benchmark, id);
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType txnType) {
      return TransactionStatus.SUCCESS;
    }
  }

  private TestWorker worker;

  @Before
  public void setUp() {
    WorkloadConfiguration workConf = new WorkloadConfiguration();
    workConf.setBenchmarkName("mockbenchmark");
    // Avoid opening a real JDBC connection in Worker's constructor.
    workConf.setNewConnectionPerTxn(true);
    MockBenchmark bench = new MockBenchmark(workConf);
    this.worker = new TestWorker(bench, 0);
  }

  private ConcurrentLinkedQueue<Integer> bufferOf(Worker<?> w) throws Exception {
    Field f = Worker.class.getDeclaredField("intervalLatenciesMicros");
    f.setAccessible(true);
    @SuppressWarnings("unchecked")
    ConcurrentLinkedQueue<Integer> q = (ConcurrentLinkedQueue<Integer>) f.get(w);
    return q;
  }

  @Test
  public void testDrainEmptyReturnsEmptyArray() {
    int[] drained = worker.drainIntervalLatenciesMicros();
    assertEquals(0, drained.length);
  }

  @Test
  public void testDrainReturnsAndClearsSamples() throws Exception {
    ConcurrentLinkedQueue<Integer> q = bufferOf(worker);
    q.offer(100);
    q.offer(200);
    q.offer(50);

    int[] drained = worker.drainIntervalLatenciesMicros();
    // The drain preserves insertion order.
    assertArrayEquals(new int[] {100, 200, 50}, drained);

    // Buffer is cleared after drain.
    int[] second = worker.drainIntervalLatenciesMicros();
    assertEquals(0, second.length);
  }

  @Test
  public void testDrainConcurrentPushesDoNotLoseSamples() throws Exception {
    final ConcurrentLinkedQueue<Integer> q = bufferOf(worker);
    final int producerSamples = 10_000;
    final AtomicBoolean stopDraining = new AtomicBoolean(false);
    final java.util.List<Integer> allDrained = new java.util.ArrayList<>(producerSamples);
    final CountDownLatch producerDone = new CountDownLatch(1);

    Thread producer =
        new Thread(
            () -> {
              for (int i = 1; i <= producerSamples; ++i) {
                q.offer(i);
              }
              producerDone.countDown();
            });

    Thread drainer =
        new Thread(
            () -> {
              while (!stopDraining.get()) {
                for (int v : worker.drainIntervalLatenciesMicros()) {
                  allDrained.add(v);
                }
              }
              // Final drain after signaled to stop.
              for (int v : worker.drainIntervalLatenciesMicros()) {
                allDrained.add(v);
              }
            });

    producer.start();
    drainer.start();
    assertTrue(producerDone.await(5, TimeUnit.SECONDS));
    // Give the drainer a moment to observe any trailing items, then stop it.
    Thread.sleep(20);
    stopDraining.set(true);
    drainer.join(TimeUnit.SECONDS.toMillis(5));

    assertEquals(producerSamples, allDrained.size());
    // Because a single producer pushes 1..N in order and a single drainer observes in order, the
    // drained values should equal [1..N].
    int[] got = allDrained.stream().mapToInt(Integer::intValue).toArray();
    int[] expected = new int[producerSamples];
    for (int i = 0; i < producerSamples; ++i) {
      expected[i] = i + 1;
    }
    assertArrayEquals(expected, got);
    // And no residual items.
    assertEquals(0, worker.drainIntervalLatenciesMicros().length);
  }
}
