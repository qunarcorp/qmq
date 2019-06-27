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

package qunar.tc.qmq.producer;

import com.google.common.base.Charsets;
import com.google.common.io.CharSink;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author keli.wang
 * @since 2019-06-20
 */
class BatchFileAppender implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(BatchFileAppender.class);

    private final CharSink sink;
    private final ArrayBlockingQueue<String> pendingWrite;
    private final Thread batchWriteThread;
    private volatile boolean stop;

    BatchFileAppender(final File file, final int capacity) {
        this.sink = Files.asCharSink(file, Charsets.UTF_8, FileWriteMode.APPEND);
        this.pendingWrite = new ArrayBlockingQueue<>(capacity);
        this.batchWriteThread = new Thread(new Runnable() {
            @Override
            public void run() {
                batchWriteLoop();
            }
        });
        this.stop = false;
        this.batchWriteThread.start();
    }

    private void batchWriteLoop() {
        while (!stop || pendingWrite.size() > 0) {
            final ArrayList<String> c = new ArrayList<>(1000);
            pendingWrite.drainTo(c);
            if (c.isEmpty()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            writeBatch(sink, c);
        }
        LOG.info("batch write thread stopped. pendingWrite size: {}", pendingWrite.size());
    }

    private void writeBatch(final CharSink sink, final List<String> batch) {
        try {
            sink.writeLines(batch);
        } catch (IOException e) {
            LOG.error("write lines failed.", e);
        }
    }

    public void write(final String data) {
        try {
            pendingWrite.put(data);
        } catch (InterruptedException e) {
            LOG.error("put pending write interrupted", e);
        }
    }

    @Override
    public void close() throws Exception {
        stop = true;
        batchWriteThread.join();
    }
}
