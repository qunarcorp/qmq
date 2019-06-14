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

package qunar.tc.qmq.web;


import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.LongSerializationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.RemoteMessageQuery;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.store.GetMessageResult;
import qunar.tc.qmq.store.GetMessageStatus;
import qunar.tc.qmq.store.buffer.Buffer;
import qunar.tc.qmq.store.Storage;
import qunar.tc.qmq.utils.Bytes;

import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by zhaohui.yu
 * 3/14/19
 */
public class QueryMessageServlet extends HttpServlet {
    private static final Logger LOG = LoggerFactory.getLogger(QueryMessageServlet.class);

    private static final Gson serializer = new GsonBuilder().setLongSerializationPolicy(LongSerializationPolicy.STRING).create();

    private Storage store;
    private final ExecutorService threadPoolExecutor;

    public QueryMessageServlet(DynamicConfig config, Storage store) {
        this.store = store;
        this.threadPoolExecutor = new ThreadPoolExecutor(1, config.getInt("query.max.threads", 5)
                , 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(1000), new NamedThreadFactory("query-msg"));
    }

    @Override
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) {
        resp.setStatus(HttpServletResponse.SC_OK);
        final String queryJson = req.getParameter("backupQuery");
        if (Strings.isNullOrEmpty(queryJson)) return;
        final AsyncContext context = req.startAsync();
        RemoteMessageQuery query = deserialize(queryJson);
        if (query == null) {
            context.complete();
            return;
        }

        final ServletResponse response = context.getResponse();
        final CompletableFuture<Boolean> future = query(query, response);
        future.exceptionally(throwable -> {
            LOG.error("Failed to query messages. {}", query, throwable);
            return true;
        }).thenAccept(aBoolean -> context.complete());
    }

    private RemoteMessageQuery deserialize(String json) {
        try {
            return serializer.fromJson(json, RemoteMessageQuery.class);
        } catch (JsonSyntaxException e) {
            LOG.error("Deserialize query json error.", e);
            return null;
        }
    }

    private CompletableFuture<Boolean> query(RemoteMessageQuery query, ServletResponse response) {
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        try {
            threadPoolExecutor.execute(() -> {
                try {
                    final String subject = query.getSubject();
                    final List<RemoteMessageQuery.MessageKey> keys = query.getKeys();

                    final ServletOutputStream os = response.getOutputStream();
                    for (RemoteMessageQuery.MessageKey key : keys) {
                        long sequence = key.getSequence();
                        GetMessageResult result = store.getMessage(subject, sequence);
                        if (result.getStatus() != GetMessageStatus.SUCCESS) continue;
                        try {
                            final byte[] sequenceBytes = Bytes.long2bytes(sequence);
                            final List<Buffer> buffers = result.getBuffers();
                            for (Buffer buffer : buffers) {
                                os.write(sequenceBytes);
                                ByteBuffer byteBuffer = buffer.getBuffer();
                                byte[] arr = new byte[byteBuffer.remaining()];
                                byteBuffer.get(arr);
                                os.write(arr);
                            }
                        } finally {
                            result.release();
                        }
                    }
                    os.flush();
                    os.close();
                    future.complete(true);
                } catch (IOException e) {
                    future.completeExceptionally(e);
                }
            });
        } catch (RejectedExecutionException e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            threadPoolExecutor.awaitTermination(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
        }
    }
}
