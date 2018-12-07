/*
 * Copyright 2018 Qunar
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
 * limitations under the License.com.qunar.pay.trade.api.card.service.usercard.UserCardQueryFacade
 */

package qunar.tc.qmq.store;

import io.netty.util.internal.NativeLibraryLoader;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileDescriptor;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;

public class MmapUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(MmapUtil.class);

    private static Field fdField;

    private static boolean inited = true;

    static {
        try {
            NativeLibraryLoader.load("mmaputil", PlatformDependent.getClassLoader(MmapUtil.class));
            Class<FileDescriptor> clazz = FileDescriptor.class;
            fdField = clazz.getDeclaredField("fd");
            fdField.setAccessible(true);
        } catch (Exception e) {
            inited = false;
            LOGGER.error("init failed", e);
        }
    }

    public static void free(RandomAccessFile file) {
        if (!inited) return;

        try {
            FileDescriptor fd = file.getFD();
            int intFd = (Integer) fdField.get(fd);
            free0(intFd, 0, file.length());
        } catch (Exception e) {
            LOGGER.error("free error", e);
        }
    }

    private native static void free0(int fd, long offset, long length);
}
