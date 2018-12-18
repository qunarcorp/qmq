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

package qunar.tc.qmq.util;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

/**
 * @author yiqun.fan create on 17-8-29.
 */
public class ChannelUtil {
    private static final AttributeKey<Object> DEFAULT_ATTRIBUTE = AttributeKey.valueOf("default");

    public static Object getAttribute(Channel channel) {
        synchronized (channel) {
            Attribute<Object> attr = channel.attr(DEFAULT_ATTRIBUTE);
            return attr != null ? attr.get() : null;
        }
    }

    public static boolean setAttributeIfAbsent(Channel channel, Object o) {
        synchronized (channel) {
            Attribute<Object> attr = channel.attr(DEFAULT_ATTRIBUTE);
            if (attr == null || attr.get() == null) {
                channel.attr(DEFAULT_ATTRIBUTE).set(o);
                return true;
            }
            return false;
        }
    }

    public static void removeAttribute(Channel channel) {
        synchronized (channel) {
            channel.attr(DEFAULT_ATTRIBUTE).set(null);
        }
    }
}
