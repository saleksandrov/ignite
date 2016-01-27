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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Test getEntry and getEntries methods.
 */
public class CacheGetEntrySeltTest extends GridCacheAbstractSelfTest {

    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(null);

        return cfg;
    }

    /** */
    public void testNear() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setName("near");
        cfg.setNearConfiguration(new NearCacheConfiguration());

        test(cfg);
    }

    /** */
    public void testNearTransactional() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setName("nearT");
        cfg.setNearConfiguration(new NearCacheConfiguration());

        testT(cfg);
    }

    /** */
    public void testPartitioned() {
        CacheConfiguration cfg = new CacheConfiguration();
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setName("partitioned");

        test(cfg);
    }

    /** */
    public void testPartitionedTransactional() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setName("partitionedT");

        testT(cfg);
    }

    /** */
    public void testLocal() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(CacheMode.LOCAL);
        cfg.setName("local");

        test(cfg);
    }

    /** */
    public void testLocalTransactional() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(CacheMode.LOCAL);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setName("localT");

        testT(cfg);
    }

    /** */
    public void testReplicated() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(CacheMode.REPLICATED);
        cfg.setName("replicated");

        test(cfg);
    }

    /** */
    public void testReplicatedTransactional() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(CacheMode.REPLICATED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setName("replicatedT");

        testT(cfg);
    }

    /** */
    private void testT(CacheConfiguration cfg) {
        Ignite ignite = grid(0);

        try (IgniteCache<Integer, TestValue> cache = ignite.createCache(cfg)) {
            init(cache);

            for (TransactionConcurrency txConcurrency : TransactionConcurrency.values()) {
                for (TransactionIsolation txIsolation : TransactionIsolation.values()) {
                    try (Transaction tx = ignite.transactions().txStart(txConcurrency, txIsolation, 100000, 1000)) {
                        try {
                            testGetEntry(cache);

                            tx.commit();
                        }
                        catch (Exception e) {
                            assert txIsolation == TransactionIsolation.REPEATABLE_READ ||
                                (txIsolation == TransactionIsolation.SERIALIZABLE &&
                                    tx.concurrency() == TransactionConcurrency.PESSIMISTIC);
                        }
                    }

                    try (Transaction tx = ignite.transactions().txStart(txConcurrency, txIsolation, 100000, 1000)) {
                        try {
                            testGetEntries(cache);

                            tx.commit();
                        }
                        catch (Exception e) {
                            assert txIsolation == TransactionIsolation.REPEATABLE_READ ||
                                (txIsolation == TransactionIsolation.SERIALIZABLE &&
                                    tx.concurrency() == TransactionConcurrency.PESSIMISTIC);
                        }
                    }
                }

            }
        }
    }

    /** */
    private void test(CacheConfiguration cfg) {
        try (IgniteCache<Integer, TestValue> cache = grid(0).createCache(cfg)) {
            init(cache);

            testGetEntry(cache);

            testGetEntries(cache);
        }
    }

    /** */
    private void init(IgniteCache<Integer, TestValue> cache) {
        // Put.
        for (int i = 0; i < 100; ++i)
            cache.put(i, new TestValue(i));
    }

    /** */
    private void checkVersion(CacheEntry<Integer, ?> e, IgniteCache<Integer, TestValue> cache) {
        CacheConfiguration cfg = cache.getConfiguration(CacheConfiguration.class);

        if (cfg.getCacheMode() != CacheMode.LOCAL) {
            Ignite prim = primaryNode(e.getKey(), cache.getName());

            GridCacheAdapter<Object, Object> cacheAdapter = ((IgniteKernal)prim).internalCache(cache.getName());

            if (cfg.getNearConfiguration() != null)
                cacheAdapter = ((GridNearCacheAdapter)cacheAdapter).dht();

            IgniteCacheObjectProcessor cacheObjects = cacheAdapter.context().cacheObjects();

            CacheObjectContext cacheObjCtx = cacheAdapter.context().cacheObjectContext();

            GridCacheMapEntry me = cacheAdapter.map().getEntry(cacheObjects.toCacheKeyObject(cacheObjCtx, e.getKey(), true));

            try {
                assert me.version().equals(e.version());
            }
            catch (GridCacheEntryRemovedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    /** */
    private void testGetEntry(IgniteCache<Integer, TestValue> cache) {
        // getEntry regular.
        for (int i = 0; i < 100; ++i) {
            CacheEntry<Integer, TestValue> e = cache.getEntry(i);

            checkVersion(e, cache);

            assertEquals(e.getValue().val, i);

            assertNotNull(e.version());
        }

        IgniteCache<Integer, BinaryObject> cacheB = cache.withKeepBinary();

        // getEntry withKeepBinary.
        for (int i = 0; i < 100; ++i) {
            CacheEntry<Integer, BinaryObject> e = cacheB.getEntry(i);

            checkVersion(e, cache);

            assertEquals(((TestValue)e.getValue().deserialize()).val, i);

            assertNotNull(e.version());
        }
    }

    /** */
    private void testGetEntries(IgniteCache<Integer, TestValue> cache) {
        // getEntries regular.
        for (int i = 0; i < 100; i++) {
            Set<Integer> set = new HashSet<>();

            for (int j = 0; j < 10; j++)
                set.add(i + j);

            Collection<CacheEntry<Integer, TestValue>> es = cache.getEntries(set);

            for (CacheEntry<Integer, TestValue> e : es) {
                checkVersion(e, cache);

                assertEquals((Integer)e.getValue().val, e.getKey());

                assertTrue(set.contains(e.getValue().val));

                assertNotNull(e.version());
            }
        }

        IgniteCache<Integer, BinaryObject> cacheB = cache.withKeepBinary();

        // getEntries withKeepBinary.
        for (int i = 0; i < 100; i++) {
            Set<Integer> set = new HashSet<>();

            for (int j = 0; j < 10; j++)
                set.add(i + j);

            Collection<CacheEntry<Integer, BinaryObject>> es = cacheB.getEntries(set);

            for (CacheEntry<Integer, BinaryObject> e : es) {
                checkVersion(e, cache);

                TestValue tv = e.getValue().deserialize();

                assertEquals((Integer)tv.val, e.getKey());

                assertTrue(set.contains((tv).val));

                assertNotNull(e.version());
            }
        }
    }

    /**
     *
     */
    private static class TestValue implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        public TestValue(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestValue.class, this);
        }
    }
}
