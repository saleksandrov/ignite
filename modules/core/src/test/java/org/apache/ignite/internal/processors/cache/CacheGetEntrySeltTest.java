package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;

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

        CacheConfiguration cacheCfg1 = new CacheConfiguration();
        cacheCfg1.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg1.setName("near");
        cacheCfg1.setNearConfiguration(new NearCacheConfiguration());

        CacheConfiguration cacheCfg1t = new CacheConfiguration();
        cacheCfg1t.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg1t.setAtomicityMode(TRANSACTIONAL);
        cacheCfg1t.setName("nearT");
        cacheCfg1t.setNearConfiguration(new NearCacheConfiguration());

        CacheConfiguration cacheCfg2 = new CacheConfiguration();
        cacheCfg2.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg2.setName("partitioned");

        CacheConfiguration cacheCfg2t = new CacheConfiguration();
        cacheCfg2t.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg2t.setAtomicityMode(TRANSACTIONAL);
        cacheCfg2t.setName("partitionedT");

        CacheConfiguration cacheCfg3 = new CacheConfiguration();
        cacheCfg3.setCacheMode(CacheMode.LOCAL);
        cacheCfg3.setName("local");

        CacheConfiguration cacheCfg3t = new CacheConfiguration();
        cacheCfg3t.setCacheMode(CacheMode.LOCAL);
        cacheCfg3t.setAtomicityMode(TRANSACTIONAL);
        cacheCfg3t.setName("localT");

        CacheConfiguration cacheCfg4 = new CacheConfiguration();
        cacheCfg4.setCacheMode(CacheMode.REPLICATED);
        cacheCfg4.setName("replicated");

        CacheConfiguration cacheCfg4t = new CacheConfiguration();
        cacheCfg4t.setCacheMode(CacheMode.REPLICATED);
        cacheCfg4t.setAtomicityMode(TRANSACTIONAL);
        cacheCfg4t.setName("replicatedT");

        cfg.setMarshaller(null);

        cfg.setCacheConfiguration(cfg.getCacheConfiguration()[0], cacheCfg1, cacheCfg1t, cacheCfg2, cacheCfg2t,
            cacheCfg3, cacheCfg3t, cacheCfg4, cacheCfg4t);

        return cfg;
    }

    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetEntry() throws Exception {
        test0("near");
        test0("nearT");
        test0("partitioned");
        test0("partitionedT");
        test0("local");
        test0("localT");
        test0("replicated");
        test0("replicatedT");
    }

    private void test0(String name) {
        IgniteCache<Integer, TestValue> cache = grid(0).cache(name);

        // Put.
        for (int i = 0; i < 10_000; ++i)
            cache.put(i, new TestValue(i));

        // getEntry regular.
        for (int i = 0; i < 10_000; ++i) {
            CacheEntry<Integer, TestValue> e = cache.getEntry(i);

            assertEquals(e.getValue().val, i);

            assertNotNull(e.version());
        }

        // getEntries regular.
        for (int i = 0; i < 10_000; ++i) {
            Set<Integer> set = new HashSet<>();

            for (int j = 0; j < 10; j++)
                set.add(++i);

            Collection<CacheEntry<Integer, TestValue>> es = cache.getEntries(set);

            for (CacheEntry<Integer, TestValue> e : es) {
                assertEquals((Integer)e.getValue().val, e.getKey());

                assertTrue(e.getValue().val <= i);
                assertTrue(e.getValue().val > i - 10);

                assertNotNull(e.version());
            }
        }

        IgniteCache<Integer, BinaryObject> cacheB = grid(0).cache(name).withKeepBinary();

        // getEntry withKeepBinary.
        for (int i = 0; i < 10_000; ++i) {
            CacheEntry<Integer, BinaryObject> e = cacheB.getEntry(i);

            assertEquals(((TestValue)e.getValue().deserialize()).val, i);

            assertNotNull(e.version());
        }

        // getEntries withKeepBinary.
        for (int i = 0; i < 10_000; ++i) {
            Set<Integer> set = new HashSet<>();

            for (int j = 0; j < 10; j++)
                set.add(++i);

            Collection<CacheEntry<Integer, BinaryObject>> es = cacheB.getEntries(set);

            for (CacheEntry<Integer, BinaryObject> e : es) {
                TestValue tv = e.getValue().deserialize();

                assertEquals((Integer)tv.val, e.getKey());

                assertTrue((tv).val <= i);
                assertTrue((tv).val > i - 10);

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
