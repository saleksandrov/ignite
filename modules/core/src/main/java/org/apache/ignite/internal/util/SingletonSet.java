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
import org.jetbrains.annotations.NotNull;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Singleton set.
 */
public class SingletonSet<E> extends AbstractSet<E> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Factory method.
     *
     * @param elem Element.
     * @return Singleton set.
     */
    public static <T> SingletonSet<T> create(T elem) {
        return new SingletonSet<>(elem);
    }

    /** Element. */
    private final E elem;

    /**
     * Constructor.
     * @param elem Element.
     */
    SingletonSet(E elem) {
        this.elem = elem;
    }

    /**
     * Get element.
     *
     * @return Element.
     */
    public E element() {
        return elem;
    }

    /** {@inheritDoc} */
    @NotNull public Iterator<E> iterator() {
        return new Iterator<E>() {
            private boolean hasNext = true;

            @Override public boolean hasNext() {
                return hasNext;
            }

            @Override public E next() {
                if (hasNext) {
                    hasNext = false;

                    return elem;
                }
                else
                    throw new NoSuchElementException();
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /** {@inheritDoc} */
    public int size() {
        return 1;
    }

    /** {@inheritDoc} */
    public boolean contains(Object o) {
        return F.eq(o, elem);
    }
}
