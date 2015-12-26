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

package org.apache.ignite.internal.processors.cache.distributed;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.SingletonSet;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction node mapping.
 */
public class GridDistributedTxMapping implements Externalizable {
    /** Empty entries. */
    private static final Collection<IgniteTxEntry> EMPTY = Collections.emptySet();

    /** */
    private static final long serialVersionUID = 0L;

    /** Mapped node. */
    @GridToStringExclude
    private ClusterNode node;

    /** Entries. */
    @GridToStringInclude
    private Collection<IgniteTxEntry> entries;

    /** Explicit lock flag. */
    private boolean explicitLock;

    /** DHT version. */
    private GridCacheVersion dhtVer;

    /** {@code True} if this is last mapping for node. */
    private boolean last;

    /** {@code True} if mapping is for near caches, {@code false} otherwise. */
    private boolean near;

    /** {@code True} if this is first mapping for optimistic tx on client node. */
    private boolean clientFirst;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDistributedTxMapping() {
        // No-op.
    }

    /**
     * @param node Mapped node.
     */
    public GridDistributedTxMapping(ClusterNode node) {
        this.node = node;

        entries = EMPTY;
    }

    /**
     * @return {@code True} if this is last mapping for node.
     */
    public boolean last() {
        return last;
    }

    /**
     * @param last If {@code True} this is last mapping for node.
     */
    public void last(boolean last) {
        this.last = last;
    }

    /**
     * @return {@code True} if this is first mapping for optimistic tx on client node.
     */
    public boolean clientFirst() {
        return clientFirst;
    }

    /**
     * @param clientFirst {@code True} if this is first mapping for optimistic tx on client node.
     */
    public void clientFirst(boolean clientFirst) {
        this.clientFirst = clientFirst;
    }

    /**
     * @return {@code True} if mapping is for near caches, {@code false} otherwise.
     */
    public boolean near() {
        return near;
    }

    /**
     * @param near {@code True} if mapping is for near caches, {@code false} otherwise.
     */
    public void near(boolean near) {
        this.near = near;
    }

    /**
     * @return Node.
     */
    public ClusterNode node() {
        return node;
    }

    /**
     * @return Entries.
     */
    public Collection<IgniteTxEntry> entries() {
        return entries;
    }

    /**
     * @return {@code True} if lock is explicit.
     */
    public boolean explicitLock() {
        return explicitLock;
    }

    /**
     * Sets explicit flag to {@code true}.
     */
    public void markExplicitLock() {
        explicitLock = true;
    }

    /**
     * @return DHT version.
     */
    public GridCacheVersion dhtVersion() {
        return dhtVer;
    }

    /**
     * @param dhtVer DHT version.
     * @param writeVer DHT writeVersion.
     */
    public void dhtVersion(GridCacheVersion dhtVer, GridCacheVersion writeVer) {
        this.dhtVer = dhtVer;

        for (IgniteTxEntry e : entries)
            e.dhtVersion(writeVer);
    }

    /**
     * @return Reads.
     */
    public Collection<IgniteTxEntry> reads() {
        return F.view(entries, CU.reads());
    }

    /**
     * @return Writes.
     */
    public Collection<IgniteTxEntry> writes() {
        return F.view(entries, CU.writes());
    }

    /**
     * @param entry Adds entry.
     */
    public void add(IgniteTxEntry entry) {
        if (entries == EMPTY) {
            entries = SingletonSet.create(entry);

            return;
        }
        else if (entries instanceof SingletonSet) {
            Collection<IgniteTxEntry> entries0 = new LinkedHashSet<>();

            entries0.add(((SingletonSet<IgniteTxEntry>)entries).element());

            entries = entries0;
        }

        entries.add(entry);
    }

    /**
     * @param entry Entry to remove.
     * @return {@code True} if entry was removed.
     */
    public boolean removeEntry(IgniteTxEntry entry) {
        if (entries != EMPTY) {
            if (F.eq(entry, ((SingletonSet<IgniteTxEntry>)entries).element())) {
                entries = EMPTY;

                return true;
            }
            else
                return entries.remove(entry);
        }

        return false;
    }

    /**
     * Remove invalid partitions.
     *
     * @param invalidParts Invalid partitions.
     */
    public void removeInvalidPartitions(Collection<Integer> invalidParts) {
        if (entries != EMPTY) {
            if (entries instanceof SingletonSet) {
                if (invalidParts.contains(((SingletonSet<IgniteTxEntry>)entries).element().cached().partition()))
                    entries = EMPTY;
            }
            else {
                for (Iterator<IgniteTxEntry> it = entries.iterator(); it.hasNext();) {
                    IgniteTxEntry entry  = it.next();

                    if (invalidParts.contains(entry.cached().partition()))
                        it.remove();
                }
            }
        }
    }

    /**
     * Remove invalid partitions by cache ID.
     *
     * @param invalidPartsMap Invalid partitions map.
     */
    public void removeInvalidPartitionsByCacheId(Map<Integer, int[]> invalidPartsMap) {
        if (entries != EMPTY) {
            if (entries instanceof SingletonSet) {
                IgniteTxEntry entry = ((SingletonSet<IgniteTxEntry>)entries).element();

                int[] invalidParts = invalidPartsMap.get(entry.cacheId());

                if (invalidParts != null && F.contains(invalidParts, entry.cached().partition()))
                    entries = EMPTY;
            }
            else {
                for (Iterator<IgniteTxEntry> it = entries.iterator(); it.hasNext();) {
                    IgniteTxEntry entry  = it.next();

                    int[] invalidParts = invalidPartsMap.get(entry.cacheId());

                    if (invalidParts != null && F.contains(invalidParts, entry.cached().partition()))
                        it.remove();
                }
            }
        }
    }

    /**
     * @param keys Keys to evict readers for.
     */
    public void evictReaders(@Nullable Collection<IgniteTxKey> keys) {
        if (keys == null || keys.isEmpty())
            return;

        if (entries != EMPTY) {
            if (entries instanceof SingletonSet) {
                if (keys.contains(((SingletonSet<IgniteTxEntry>)entries).element().txKey()))
                    entries = EMPTY;
            }
            else {
                for (Iterator<IgniteTxEntry> it = entries.iterator(); it.hasNext(); ) {
                    IgniteTxEntry entry = it.next();

                    if (keys.contains(entry.txKey()))
                        it.remove();
                }
            }
        }
    }

    /**
     * Whether empty or not.
     *
     * @return Empty or not.
     */
    public boolean empty() {
        return entries.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(node);

        U.writeCollection(out, entries);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        node = (ClusterNode)in.readObject();

        int size = in.readInt();

        if (size <= 0)
            entries = EMPTY;
        else if (size == 1)
            entries = SingletonSet.create((IgniteTxEntry)in.readObject());
        else {
            entries = U.newLinkedHashSet(size);

            for (int i = 0; i < size; i++)
                entries.add((IgniteTxEntry)in.readObject());
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDistributedTxMapping.class, this, "node", node.id());
    }
}
