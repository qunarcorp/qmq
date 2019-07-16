/*
 * Copyright 2019 Qunar, Inc.
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

import com.diffblue.deeptestutils.Reflector;

import java.lang.reflect.InvocationTargetException;

import org.junit.Assert;
import org.junit.Test;

public class LongHashSetTest {

    @Test
    public void testSet1() {
        Assert.assertTrue(new LongHashSet(2).set(-1));
    }

    @Test
    public void testSet2() throws InvocationTargetException {
        LongHashSet longHashSet = (LongHashSet) Reflector
                .getInstance("qunar.tc.qmq.delay.base.LongHashSet");
        Reflector.setField(longHashSet, "values", new long[]{1L, -1L});
        Assert.assertTrue(longHashSet.set(2L));
    }

    @Test
    public void testContains() {
        Assert.assertFalse(new LongHashSet(2).contains(-1));
        Assert.assertFalse(new LongHashSet(2).contains(2));
    }

    @Test
    public void testContains2() throws InvocationTargetException {
        LongHashSet longHashSet = (LongHashSet) Reflector
                .getInstance("qunar.tc.qmq.delay.base.LongHashSet");
        Reflector.setField(longHashSet, "values", new long[]{1L, -1L});

        Assert.assertFalse(longHashSet.contains(1L));
        Assert.assertFalse(longHashSet.contains(2L));
    }

    @Test
    public void testContains3() throws InvocationTargetException {
        LongHashSet objectUnderTest = (LongHashSet) Reflector
                .getInstance("qunar.tc.qmq.delay.base.LongHashSet");
        Reflector.setField(objectUnderTest, "values", new long[]{0L});

        Assert.assertTrue(objectUnderTest.contains(0L));
    }

    @Test
    public void testSize() {
        LongHashSet longHashSet1 = new LongHashSet(0);
        longHashSet1.set(2L);
        Assert.assertEquals(1, longHashSet1.size());

        LongHashSet longHashSet2 = new LongHashSet(0);
        longHashSet2.set(-1L);
        Assert.assertEquals(1, longHashSet2.size());
    }
}
