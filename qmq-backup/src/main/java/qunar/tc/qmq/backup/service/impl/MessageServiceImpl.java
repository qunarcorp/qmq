package qunar.tc.qmq.backup.service.impl;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.LongSerializationPolicy;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.*;
import qunar.tc.qmq.backup.service.MessageService;
import qunar.tc.qmq.backup.service.SlaveMetaSupplier;
import qunar.tc.qmq.backup.store.MessageStore;
import qunar.tc.qmq.backup.store.RecordStore;
import qunar.tc.qmq.backup.util.Tags;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.RemoteMessageQuery;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.protocol.QMQSerializer;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/29
 */
public class MessageServiceImpl implements MessageService {
    private static final Logger LOG = LoggerFactory.getLogger(MessageServiceImpl.class);

    private static final String MESSAGE_QUERY_PROTOCOL = "http://";
    private static final String MESSAGE_QUERY_URL = "/api/broker/message";

    private static final AsyncHttpClient ASYNC_HTTP_CLIENT = new AsyncHttpClient();

    private final Gson serializer = new GsonBuilder().setLongSerializationPolicy(LongSerializationPolicy.STRING).create();

    private static final MessageService MESSAGE_SERVICE = new MessageServiceImpl();

    private MessageStore indexStore;
    private MessageStore deadStore;
    private RecordStore recordStore;
    private SlaveMetaSupplier metaSupplier;

    private MessageServiceImpl() {
    }

    public static MessageService getInstance() {
        return MESSAGE_SERVICE;
    }

    public void init(DynamicConfig config, MessageStore indexStore, MessageStore deadStore, RecordStore recordStore) {
        this.indexStore = indexStore;
        this.deadStore = deadStore;
        this.recordStore = recordStore;
        this.metaSupplier = new SlaveMetaSupplier(config);
    }

    @Override
    public ResultIterable<BackupMessage> findMessages(BackupQuery query) {
        return indexStore.findMessages(query);
    }

    @Override
    public ResultIterable<BackupMessage> findDeadMessages(BackupQuery query) {
        return deadStore.findMessages(query);
    }

    @Override
    public BackupMessage findMessage(BackupQuery query) {
        try {
            String subject = query.getSubject();
            String brokerGroup = query.getBrokerGroup();
            long sequence = query.getSequence();
            BackupMessageMeta meta = new BackupMessageMeta(sequence, brokerGroup, "");
            return retrieveMessageWithMeta(brokerGroup, subject, Lists.newArrayList(meta)).get(0);
        } catch (Exception e) {
            LOG.error("Failed to find message details. {} ", query, e);
            return null;
        }
    }

    private List<BackupMessage> retrieveMessageWithMeta(String brokerGroup, String subject, List<BackupMessageMeta> metas) {
        LOG.info("retrieve message from {}", brokerGroup);
        final String backupAddress = metaSupplier.resolveServerAddress(brokerGroup);
        if (Strings.isNullOrEmpty(backupAddress)) return Collections.emptyList();
        String url = MESSAGE_QUERY_PROTOCOL + backupAddress + MESSAGE_QUERY_URL;

        try {
            final AsyncHttpClient.BoundRequestBuilder builder = ASYNC_HTTP_CLIENT.prepareGet(url);
            builder.addQueryParam("backupQuery", serializer.toJson(getQuery(subject, metas)));

            final Response response = builder.execute().get();
            if (response.getStatusCode() != HttpResponseStatus.OK.code()) {
                return Collections.emptyList();
            }

            List<BackupMessage> messages = Lists.newArrayList();
            final ByteBuffer buffer = response.getResponseBodyAsByteBuffer();
            while (buffer.hasRemaining()) {
                if (buffer.remaining() < Long.BYTES) break;
                long sequence = buffer.getLong();
                BackupMessage message = decodeBackupMessage(buffer, sequence);
                if (message != null) {
                    message.setBrokerGroup(brokerGroup);
                    messages.add(message);
                }
            }

            return messages;
        } catch (InterruptedException | ExecutionException | IOException e) {
            LOG.error("retrieve message with meta failed.", e);
            throw new RuntimeException("retrieve message failed.");
        }
    }

    private BackupMessage decodeBackupMessage(final ByteBuffer message, final long messageSeq) {
        // flag
        byte flag = message.get();
        // createTime
        final long createTime = message.getLong();
        // expiredTime or scheduleTime
        message.position(message.position() + Long.BYTES);
        // subject
        final String subject = PayloadHolderUtils.readString(message);
        // messageId
        final String messageId = PayloadHolderUtils.readString(message);
        // tags
        Set<String> tags = Tags.readTags(flag, message);
        // body
        final byte[] bodyBs = PayloadHolderUtils.readBytes(message);
        HashMap<String, Object> attributes = null;
        try {
            attributes = getAttributes(bodyBs, createTime);
        } catch (Exception e) {
            LOG.error("retrieve message attributes failed.", e);
        }
        BackupMessage backupMessage = new BackupMessage(messageId, subject);
        backupMessage.setAttrs(attributes);
        tags.forEach(backupMessage::addTag);
        backupMessage.setSequence(messageSeq);

        return backupMessage;
    }

    private static HashMap<String, Object> getAttributes(final byte[] bodyBs, final long createTime) {
        HashMap<String, Object> attributes;
        attributes = QMQSerializer.deserializeMap(bodyBs);
        attributes.put(BaseMessage.keys.qmq_createTime.name(), createTime);
        return attributes;
    }

    private RemoteMessageQuery getQuery(String subject, List<BackupMessageMeta> metas) {
        final List<RemoteMessageQuery.MessageKey> keys = Lists.newArrayListWithCapacity(metas.size());
        for (BackupMessageMeta meta : metas) {
            keys.add(new RemoteMessageQuery.MessageKey(meta.getSequence()));
        }
        return new RemoteMessageQuery(subject, keys);
    }

    @Override
    public RecordResult findRecords(RecordQuery query) {
        return recordStore.findRecords(query);
    }
}
