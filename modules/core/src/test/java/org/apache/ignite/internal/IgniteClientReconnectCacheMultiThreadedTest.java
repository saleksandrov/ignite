/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Client reconnect test in multi threaded mode while cache operations are in progress.
 */
public class IgniteClientReconnectCacheMultiThreadedTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 14;

    /** */
    private static final int CLIENT_GRID_CNT = 14;

    /** */
    private static volatile boolean clientMode;

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * @throws Exception If fails.
     */
    public IgniteClientReconnectCacheMultiThreadedTest() throws Exception {
        super(false);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional"})
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (clientMode)
            cfg.setClientMode(true);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60 * 1000;
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testMassiveServersShutdown() throws Exception {
        clientMode = false;

        final int serversToKill = GRID_CNT / 2;

        startGridsMultiThreaded(GRID_CNT);

        clientMode = true;

        startGridsMultiThreaded(GRID_CNT, CLIENT_GRID_CNT);

        final AtomicBoolean done = new AtomicBoolean();

        // Starting a cache dynamically.
        Ignite client = grid(GRID_CNT);

        assertTrue(client.configuration().isClientMode());

        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cfg.setBackups(2);
        cfg.setOffHeapMaxMemory(0);
        cfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);

        IgniteCache cache = client.getOrCreateCache(cfg);

        HashMap<String, Integer> put = new HashMap<>();

        // Preloading the cache with some data.
        for (int i = 0; i < 10_000; i++)
            put.put(String.valueOf(i), i);

        cache.putAll(put);

        // Preparing client nodes and starting cache operations from them.
        final BlockingQueue<Integer> clientIdx = new LinkedBlockingQueue<>();

        for (int i = GRID_CNT; i < GRID_CNT + CLIENT_GRID_CNT; i++)
            clientIdx.add(i);

        IgniteInternalFuture<?> clientsFut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    int idx = clientIdx.take();

                    Ignite ignite = grid(idx);

                    assertTrue(ignite.configuration().isClientMode());

                    IgniteCache<String, Integer> cache = ignite.cache(null);

                    IgniteTransactions txs = ignite.transactions();

                    Random rand = new Random();

                    while (!done.get()) {
                        Transaction tx = txs.txStart(TransactionConcurrency.PESSIMISTIC,
                            TransactionIsolation.READ_COMMITTED);

                        try {
                            cache.put(String.valueOf(rand.nextInt(10_000)), rand.nextInt(50_000));

                            tx.commit();
                        }
                        catch (ClusterTopologyException ex) {
                            ex.retryReadyFuture().get();
                        }
                        catch (CacheException e) {
                            if (X.hasCause(e, IgniteClientDisconnectedException.class)) {
                                IgniteClientDisconnectedException cause = X.cause(e,
                                    IgniteClientDisconnectedException.class);

                                cause.reconnectFuture().get(); // Wait for reconnect.
                            }
                            else if (X.hasCause(e, ClusterTopologyException.class)) {
                                ClusterTopologyException cause = X.cause(e, ClusterTopologyException.class);

                                cause.retryReadyFuture().get();
                            }
                            else
                                throw e;
                        }
                        finally {
                            tx.close();
                        }
                    }

                    return null;
                }
            },
            CLIENT_GRID_CNT
        );

        // Killing a half of server nodes.
        final BlockingQueue<Integer> victims = new LinkedBlockingQueue<>();

        for (int i = 0; i < serversToKill; i++)
            victims.add(i);

        final BlockingQueue<Integer> assassins = new LinkedBlockingQueue<>();

        for (int i = serversToKill; i < GRID_CNT; i++)
            assassins.add(i);

        IgniteInternalFuture<?> serversShutdownFut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Thread.sleep(5_000);

                    Ignite assassin = grid(assassins.take());

                    assertFalse(assassin.configuration().isClientMode());

                    Ignite victim = grid(victims.take());

                    assertFalse(victim.configuration().isClientMode());

                    assassin.configuration().getDiscoverySpi().failNode(victim.cluster().localNode().id(), null);

                    return null;
                }
            },
            assassins.size()
        );

        serversShutdownFut.get();

        Thread.sleep(15_000);

        done.set(true);

        clientsFut.get();
    }
}
