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

package qunar.tc.qmq.backup.service.impl;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.*;
import qunar.tc.qmq.backup.service.MessageService;
import qunar.tc.qmq.backup.service.SlaveMetaSupplier;
import qunar.tc.qmq.backup.store.MessageStore;
import qunar.tc.qmq.backup.store.RecordStore;
import qunar.tc.qmq.backup.util.GsonUtils;
import qunar.tc.qmq.backup.util.HBaseValueDecoder;
import qunar.tc.qmq.base.RemoteMessageQuery;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.configuration.DynamicConfig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

import static org.asynchttpclient.Dsl.asyncHttpClient;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/29
 */
public class MessageServiceImpl implements MessageService, Disposable {
    private static final Logger LOG = LoggerFactory.getLogger(MessageServiceImpl.class);

    private static final String MESSAGE_QUERY_PROTOCOL = "http://";
    private static final String MESSAGE_QUERY_URL = "/api/broker/message";

    private static final AsyncHttpClient ASYNC_HTTP_CLIENT = asyncHttpClient();

    private final MessageStore indexStore;
    private final MessageStore deadStore;
    private final RecordStore recordStore;
    private final SlaveMetaSupplier metaSupplier;

    private final ExecutorService queryExecutorService;

    public MessageServiceImpl(DynamicConfig config, MessageStore indexStore, MessageStore deadStore, RecordStore recordStore) {
        this.indexStore = indexStore;
        this.deadStore = deadStore;
        this.recordStore = recordStore;
        this.metaSupplier = new SlaveMetaSupplier(config);
        this.queryExecutorService = new ThreadPoolExecutor(1, config.getInt("max.query.threads", 3), 1, TimeUnit.MINUTES
                , new LinkedBlockingQueue<>(1000), new NamedThreadFactory("backup-query"));
    }

    @Override
    public CompletableFuture<MessageQueryResult> findMessages(BackupQuery query) {
        final CompletableFuture<MessageQueryResult> future = new CompletableFuture<>();
        try {
            queryExecutorService.execute(() -> {
                try {
                    final MessageQueryResult result = indexStore.findMessages(query);
                    future.complete(result);
                } catch (Exception e) {
                    LOG.error("Find messages error.", e);
                    future.completeExceptionally(e);
                }
            });
        } catch (RejectedExecutionException e) {
            LOG.error("Find messages reject error.", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<MessageQueryResult> findDeadMessages(BackupQuery query) {
        final CompletableFuture<MessageQueryResult> future = new CompletableFuture<>();
        try {
            queryExecutorService.execute(() -> {
                try {
                    final MessageQueryResult result = deadStore.findMessages(query);
                    future.complete(result);
                } catch (Exception e) {
                    LOG.error("Find dead messages error.", e);
                    future.completeExceptionally(e);
                }
            });
        } catch (RejectedExecutionException e) {
            LOG.error("Find dead messages reject error.", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<BackupMessage> findMessage(BackupQuery query) {
        final CompletableFuture<BackupMessage> future = new CompletableFuture<>();
        try {
            queryExecutorService.execute(() -> {
                try {
                    final String subject = query.getSubject();
                    final String brokerGroup = query.getBrokerGroup();
                    final long sequence = query.getSequence();
                    final BackupMessageMeta meta = new BackupMessageMeta(sequence, brokerGroup, "");
                    final List<BackupMessage> messages = retrieveMessageWithMeta(brokerGroup, subject, Lists.newArrayList(meta));
                    if (messages.isEmpty()) {
                        future.complete(null);
                        return;
                    }
                    final BackupMessage message = messages.get(0);
                    future.complete(message);
                } catch (Exception e) {
                    LOG.error("Failed to find message details. {} ", query, e);
                    future.completeExceptionally(e);
                }
            });
        } catch (RejectedExecutionException e) {
            LOG.error("Find message reject error.", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<byte[]> findMessageBytes(BackupQuery query) {
        final CompletableFuture<byte[]> future = new CompletableFuture<>();
        try {
            queryExecutorService.execute(() -> {
                try {
                    final String subject = query.getSubject();
                    final String brokerGroup = query.getBrokerGroup();
                    final long sequence = query.getSequence();
                    final BackupMessageMeta meta = new BackupMessageMeta(sequence, brokerGroup, "");
                    final byte[] messageBytes = getMessageBytesWithMeta(brokerGroup, subject, Lists.newArrayList(meta));
                    if (messageBytes.length == 0) {
                        future.complete(null);
                        return;
                    }
                    future.complete(messageBytes);
                } catch (Exception e) {
                    LOG.error("Failed to find message details. {} ", query, e);
                    future.completeExceptionally(e);
                }
            });
        } catch (RejectedExecutionException e) {
            LOG.error("Find message reject error.", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    private byte[] getMessageBytesWithMeta(String brokerGroup, String subject, List<BackupMessageMeta> metas) {
        final String backupAddress = metaSupplier.resolveServerAddress(brokerGroup);
        if (Strings.isNullOrEmpty(backupAddress)) return new byte[]{};
        String url = MESSAGE_QUERY_PROTOCOL + backupAddress + MESSAGE_QUERY_URL;

        try {
            BoundRequestBuilder boundRequestBuilder = ASYNC_HTTP_CLIENT.prepareGet(url);
            boundRequestBuilder.addQueryParam("backupQuery", GsonUtils.serialize(getQuery(subject, metas)));

            final Response response = boundRequestBuilder.execute().get();
            if (response.getStatusCode() != HttpResponseStatus.OK.code()) {
                return new byte[]{};
            }

            final ByteBuffer buffer = response.getResponseBodyAsByteBuffer();


            return buffer.array();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("get message byte with meta failed.", e);
            throw new RuntimeException("get message byte failed.");
        }
    }

    private List<BackupMessage> retrieveMessageWithMeta(String brokerGroup, String subject, List<BackupMessageMeta> metas) {
        LOG.info("retrieve message from {}", brokerGroup);
        final String backupAddress = metaSupplier.resolveServerAddress(brokerGroup);
        if (Strings.isNullOrEmpty(backupAddress)) return Collections.emptyList();
        String url = MESSAGE_QUERY_PROTOCOL + backupAddress + MESSAGE_QUERY_URL;

        try {
            BoundRequestBuilder boundRequestBuilder = ASYNC_HTTP_CLIENT.prepareGet(url);
            boundRequestBuilder.addQueryParam("backupQuery", GsonUtils.serialize(getQuery(subject, metas)));

            final Response response = boundRequestBuilder.execute().get();
            if (response.getStatusCode() != HttpResponseStatus.OK.code()) {
                return Collections.emptyList();
            }

            List<BackupMessage> messages = Lists.newArrayList();
            final ByteBuffer buffer = response.getResponseBodyAsByteBuffer();
            while (buffer.hasRemaining()) {
                if (buffer.remaining() < Long.BYTES) break;
                BackupMessage message = null;
                try {
                    message = HBaseValueDecoder.getMessage(buffer);
                } catch (Exception e) {
                    LOG.error("retrieve message failed.", e);
                }
                if (message != null) {
                    message.setBrokerGroup(brokerGroup);
                    messages.add(message);
                }
            }

            return messages;
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("retrieve message with meta failed.", e);
            throw new RuntimeException("retrieve message failed.");
        }
    }


    private RemoteMessageQuery getQuery(String subject, List<BackupMessageMeta> metas) {
        final List<RemoteMessageQuery.MessageKey> keys = Lists.newArrayListWithCapacity(metas.size());
        for (BackupMessageMeta meta : metas) {
            keys.add(new RemoteMessageQuery.MessageKey(meta.getSequence()));
        }
        return new RemoteMessageQuery(subject, keys);
    }

    @Override
    public CompletableFuture<RecordQueryResult> findRecords(RecordQuery query) {
        final CompletableFuture<RecordQueryResult> future = new CompletableFuture<>();
        try {
            queryExecutorService.execute(() -> {
                final RecordQueryResult result = recordStore.findRecords(query);
                future.complete(result);
            });
        } catch (RejectedExecutionException e) {
            LOG.error("Find records reject error.", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public void destroy() {
        queryExecutorService.shutdown();
        try {
            queryExecutorService.awaitTermination(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.error("Shutdown queryExecutorService interrupted.");
        }
        metaSupplier.destroy();
        try {
            ASYNC_HTTP_CLIENT.close();
        } catch (IOException ignored) {

        }
    }
}
