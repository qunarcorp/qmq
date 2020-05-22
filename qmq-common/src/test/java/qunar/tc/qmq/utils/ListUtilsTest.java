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

package qunar.tc.qmq.utils;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class ListUtilsTest {

    @Test
    public void testContains() {
        Assert.assertTrue(ListUtils.contains(new ArrayList<>(Arrays.asList(
                new byte[0])), new byte[0]));
        Assert.assertTrue(ListUtils.contains(new ArrayList<>(Arrays.asList(
                new byte[]{1, 2, 3})), new byte[]{1, 2, 3}));

        Assert.assertFalse(ListUtils.contains(new ArrayList<>(Arrays.asList(
                new byte[]{1, 2, 3})), new byte[0]));
        Assert.assertFalse(ListUtils.contains(new ArrayList<>(Arrays.asList(
                new byte[]{1, 2, 3})), new byte[]{4, 5, 6}));
        Assert.assertFalse(ListUtils.contains(new ArrayList<>(Arrays.asList(
                new byte[]{1}, new byte[]{2}, new byte[]{3})),
                new byte[]{1, 2, 3}));
    }

    @Test
    public void testContainsAll() {
        Assert.assertTrue(ListUtils.containsAll(
                new ArrayList<>(Arrays.asList(new byte[0])),
                new ArrayList<>(Arrays.asList(new byte[0]))));
        Assert.assertTrue(ListUtils.containsAll(
                new ArrayList<>(Arrays.asList(new byte[]{1, 2})),
                new ArrayList<>(Arrays.asList(new byte[]{1, 2}))));

        Assert.assertFalse(ListUtils.containsAll(
                new ArrayList<>(Arrays.asList(new byte[0])),
                new ArrayList<>(Arrays.asList(new byte[]{1, 2}))));
        Assert.assertFalse(ListUtils.containsAll(
                new ArrayList<>(Arrays.asList(new byte[]{1, 2})),
                new ArrayList<>(Arrays.asList(new byte[]{2, 3, 4}))));
        Assert.assertFalse(ListUtils.containsAll(
                new ArrayList<>(Arrays.asList(new byte[]{1, 2})),
                new ArrayList<>(Arrays.asList(new byte[]{4, 5, 6}))));
    }

    @Test
    public void testIntersection() {
        Assert.assertTrue(ListUtils.intersection(
                new ArrayList<>(Arrays.asList(new byte[0])),
                new ArrayList<>(Arrays.asList(new byte[0]))));
        Assert.assertTrue(ListUtils.intersection(
                new ArrayList<>(Arrays.asList(new byte[]{1, 2})),
                new ArrayList<>(Arrays.asList(new byte[]{1, 2}))));

        Assert.assertFalse(ListUtils.intersection(
                new ArrayList<>(Arrays.asList(new byte[0])),
                new ArrayList<>(Arrays.asList(new byte[]{1, 2}))));
        Assert.assertFalse(ListUtils.intersection(
                new ArrayList<>(Arrays.asList(new byte[]{1, 2})),
                new ArrayList<>(Arrays.asList(new byte[]{2, 3, 4}))));
        Assert.assertFalse(ListUtils.intersection(
                new ArrayList<>(Arrays.asList(new byte[]{1, 2})),
                new ArrayList<>(Arrays.asList(new byte[]{4, 5, 6}))));
    }
}
