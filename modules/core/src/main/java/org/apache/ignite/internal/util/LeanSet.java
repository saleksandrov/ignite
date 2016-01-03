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

package org.apache.ignite.internal.util;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Lean set which contains up to a single element in a field.
 */
public class LeanSet<T> implements Set<T> {
    /** Hash set factory. */
    private static Factory HASH_SET_FACTORY = new HashSetFactory();

    /** Linked hash set factory. */
    private static Factory LINKED_HASH_SET_FACTORY = new LinkedHashSetFactory();

    /**
     * Get hash set factory.
     *
     * @return Hash set factory.
     */
    @SuppressWarnings("unchecked")
    public static <T> Factory<T> hashSetFactory() {
        return HASH_SET_FACTORY;
    }

    /**
     * Get linked hash set factory.
     *
     * @return Linked hash set factory.
     */
    @SuppressWarnings("unchecked")
    public static <T> Factory<T> linkedHashSetFactory() {
        return LINKED_HASH_SET_FACTORY;
    }

    /** Set factory. */
    private final Factory<T> factory;

    /** Target. */
    private Object target;

    /** Current state.*/
    private State state;

    /**
     * Default constructor. Falls-back to HashSet if needed.
     */
    @SuppressWarnings("unchecked")
    public LeanSet() {
        this((Factory<T>)hashSetFactory());
    }

    /**
     * Constructor.
     *
     * @param factory Factory.
     */
    public LeanSet(Factory<T> factory) {
        A.notNull(factory, "factory");

        this.factory = factory;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        switch (state) {
            case EMPTY:
                return 0;

            case SINGLE:
                return 1;

            default:
                return asSet().size();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        switch (state) {
            case EMPTY:
                return true;

            case SINGLE:
                return false;

            default:
                return asSet().isEmpty();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        switch (state) {
            case EMPTY:
                return false;

            case SINGLE:
                return F.eq(o, target);

            default:
                return asSet().contains(o);
        }
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<T> iterator() {
        switch (state) {
            case EMPTY:
                return F.emptyIterator();

            case SINGLE:
                return new SingletonIterator();

            default:
                return asSet().iterator();
        }
    }

    /** {@inheritDoc} */
    @NotNull @Override public Object[] toArray() {
        switch (state) {
            case EMPTY:
                return new Object[0];

            case SINGLE:
                return new Object[] { target };

            default:
                return asSet().toArray();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"SuspiciousToArrayCall", "unchecked", "NullableProblems"})
    @NotNull @Override public <T1> T1[] toArray(T1[] a) {

        switch (state) {
            case EMPTY:
                if (a.length > 0)
                    a[0] = null;

                return a;

            case SINGLE:
                if (a.length == 0)
                    return (T1[]) (new Object[] { target });
                else {
                    a[0] = (T1)target;

                    if (a.length > 1)
                        a[1] = null;

                    return a;
                }

            default:
                return asSet().toArray(a);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean add(T t) {
        switch (state) {
            case EMPTY:
                target = t;

                state = State.SINGLE;

                return true;

            case SINGLE:
                return !F.eq(target, t) && toInflated().add(t);

            default:
                return add(t);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        switch (state) {
            case EMPTY:
                return false;

            case SINGLE:
                if (F.eq(target, o)) {
                    toEmpty();

                    return true;
                }
                else
                    return false;

            default:
                return asSet().remove(o);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override public boolean containsAll(Collection<?> c) {
        switch (state) {
            case EMPTY:
                return false;

            case SINGLE:
                return c.size() == 1 && c.contains(target);

            default:
                return asSet().containsAll(c);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override public boolean addAll(Collection<? extends T> c) {
        switch (state) {
            case EMPTY:
            case SINGLE:
                return !c.isEmpty() && toInflated().addAll(c);

            default:
                return asSet().addAll(c);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override public boolean retainAll(Collection<?> c) {
        switch (state) {
            case EMPTY:
                return false;

            case SINGLE:
                if (!c.contains(target)) {
                    toEmpty();

                    return true;
                }
                else
                    return false;

            default:
                return asSet().retainAll(c);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override public boolean removeAll(Collection<?> c) {
        switch (state) {
            case EMPTY:
                return false;

            case SINGLE:
                if (c.contains(target)) {
                    toEmpty();

                    return true;
                }
                else
                    return false;

            default:
                return asSet().removeAll(c);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        switch (state) {
            case SINGLE:
                toEmpty();

            case INFLATED:
                asSet().clear();
        }
    }

    /**
     * Get target as object.
     *
     * @return Object.
     */
    @SuppressWarnings("unchecked")
    private T asObject() {
        assert state == State.SINGLE;

        return (T)target;
    }

    /**
     * Get target as set.
     *
     * @return Set.
     */
    @SuppressWarnings("unchecked")
    private Set<T> asSet() {
        assert state == State.INFLATED;

        return (Set<T>)target;
    }

    /**
     * Inflate to fully-fledged set.
     *
     * @return Created set.
     */
    private Set<T> toInflated() {
        assert state != State.INFLATED;

        Set<T> set = factory.create();

        if (state == State.SINGLE)
            set.add(asObject());

        state = State.INFLATED;

        return set;
    }

    /**
     * Set to empty state.
     */
    private void toEmpty() {
        target = null;

        state = State.EMPTY;
    }

    /**
     * State.
     */
    private enum State {
        /** Empty. */
        EMPTY,

        /** Single element exists. */
        SINGLE,

        /** Inflated. */
        INFLATED
    }

    /**
     * Iterator for a single object.
     */
    private class SingletonIterator implements Iterator<T> {
        /** Whether iterator is already advanced. */
        private boolean advanced;

        /** Whether element is removed. */
        private boolean removed;

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return !advanced;
        }

        /** {@inheritDoc} */
        @Override public T next() {
            if (!hasNext())
                throw new NoSuchElementException();

            checkConcurrentModification();

            advanced = true;

            return asObject();
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            if (hasNext())
                throw new IllegalStateException("next() method was not called.");

            if (removed)
                throw new IllegalStateException("remove() was already called.");

            checkConcurrentModification();

            removed = true;

            toEmpty();
        }

        /**
         * Check concurrent modification.
         */
        private void checkConcurrentModification() {
            if (state != State.SINGLE)
                throw new ConcurrentModificationException();
        }
    }

    /**
     * Factory for real backing set.
     */
    public interface Factory<T> {
        /**
         * Create a set.
         *
         * @return Set.
         */
        Set<T> create();
    }

    /**
     * Hash set factory.
     */
    private static class HashSetFactory<T> implements Factory<T> {
        /** {@inheritDoc} */
        @Override public Set<T> create() {
            return U.newHashSet(2);
        }
    }

    /**
     * Linked hash set factory.
     */
    private static class LinkedHashSetFactory<T> implements Factory<T> {
        /** {@inheritDoc} */
        @Override public Set<T> create() {
            return U.newLinkedHashSet(2);
        }
    }
}
