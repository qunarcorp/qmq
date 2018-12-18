/*
 * Copyright 2018 Qunar, Inc.
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
 */

package qunar.tc.qmq.delay.base;

import java.util.Arrays;

/**
 * Created by zhaohui.yu
 * 8/20/18
 */
public class LongHashSet {

    static final int MISSING_VALUE = -1;

    private boolean containsMissingValue;
    private final float loadFactor;
    private int resizeThreshold;
    private int sizeOfArrayValues;

    private long[] values;

    public LongHashSet(final int proposedCapacity) {
        this.loadFactor = 0.55F;
        sizeOfArrayValues = 0;
        final int capacity = findNextPositivePowerOfTwo(proposedCapacity);
        resizeThreshold = (int) (capacity * loadFactor);
        values = new long[capacity];
        Arrays.fill(values, MISSING_VALUE);
    }

    public boolean set(final long value) {
        if (value == MISSING_VALUE) {
            final boolean previousContainsMissingValue = this.containsMissingValue;
            containsMissingValue = true;
            return !previousContainsMissingValue;
        }

        final long[] values = this.values;
        final int mask = values.length - 1;
        int index = hash(value, mask);

        while (values[index] != MISSING_VALUE) {
            index = next(index, mask);
        }

        values[index] = value;
        sizeOfArrayValues++;

        if (sizeOfArrayValues > resizeThreshold) {
            increaseCapacity();
        }

        return true;
    }

    public boolean contains(final long value) {
        if (value == MISSING_VALUE) {
            return containsMissingValue;
        }

        final long[] values = this.values;
        final int mask = values.length - 1;
        int index = hash(value, mask);

        while (values[index] != MISSING_VALUE) {
            if (values[index] == value) {
                return true;
            }

            index = next(index, mask);
        }

        return false;
    }

    public static int findNextPositivePowerOfTwo(final int value) {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    public static int hash(final long value, final int mask) {
        long hash = value * 31;
        hash = (int) hash ^ (int) (hash >>> 32);

        return (int) hash & mask;
    }

    private void increaseCapacity() {
        final int newCapacity = values.length * 2;
        if (newCapacity < 0) {
            throw new IllegalStateException("max capacity reached at size=" + size());
        }

        rehash(newCapacity);
    }

    private void rehash(final int newCapacity) {
        final int capacity = newCapacity;
        final int mask = newCapacity - 1;
        resizeThreshold = (int) (newCapacity * loadFactor);

        final long[] tempValues = new long[capacity];
        Arrays.fill(tempValues, MISSING_VALUE);

        for (final long value : values) {
            if (value != MISSING_VALUE) {
                int newHash = hash(value, mask);
                while (tempValues[newHash] != MISSING_VALUE) {
                    newHash = ++newHash & mask;
                }

                tempValues[newHash] = value;
            }
        }

        values = tempValues;
    }


    private static int next(final int index, final int mask) {
        return (index + 1) & mask;
    }

    public int size() {
        return sizeOfArrayValues + (containsMissingValue ? 1 : 0);
    }
}