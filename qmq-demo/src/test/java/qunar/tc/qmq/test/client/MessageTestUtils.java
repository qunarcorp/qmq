package qunar.tc.qmq.test.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.BaseMessage.keys;
import qunar.tc.qmq.common.JsonUtils;
import qunar.tc.qmq.producer.MessageProducerProvider;

/**
 * @author zhenwei.liu
 * @since 2019-10-14
 */
public class MessageTestUtils {

    private static final String CLASS_PATH_ROOT = MessageTestUtils.class.getClassLoader().getResource("").getPath();
    private static final String PRODUCER_ROOT = CLASS_PATH_ROOT + File.separator + "producer";
    private static final String CONSUMER_ROOT = CLASS_PATH_ROOT + File.separator + "consumer";

    private static final String BEST_TRIED_GENERATE_MESSAGE_FILE =
            PRODUCER_ROOT + File.separator + "best_tried_generate_messages";
    private static final String BEST_TRIED_SEND_MESSAGE_FILE =
            PRODUCER_ROOT + File.separator + "best_tried_send_messages";

    private static final String STRICT_GENERATE_MESSAGE_FILE =
            PRODUCER_ROOT + File.separator + "strict_generate_messages";
    private static final String STRICT_SEND_MESSAGE_FILE = PRODUCER_ROOT + File.separator + "strict_send_messages";

    private static final String DELAY_GENERATE_MESSAGE_FILE = PRODUCER_ROOT + File.separator + "delay_generate_messages";
    private static final String DELAY_SEND_MESSAGE_FILE = PRODUCER_ROOT + File.separator + "delay_send_messages";

    private static final String SHARED_BEST_TRIED_CONSUMER_FILE =
            CONSUMER_ROOT + File.separator + "shared_best_tried_consumer_messages";
    private static final String SHARED_STRICT_CONSUMER_FILE =
            CONSUMER_ROOT + File.separator + "shared_strict_consumer_messages";
    private static final String EXCLUSIVE_BEST_TRIED_CONSUMER_FILE =
            CONSUMER_ROOT + File.separator + "exclusive_best_tried_consumer_messages";
    private static final String EXCLUSIVE_STRICT_CONSUMER_FILE =
            CONSUMER_ROOT + File.separator + "exclusive_strict_consumer_messages";

    public static final File bestTriedGenerateMessageFile = mkDir(new File(BEST_TRIED_GENERATE_MESSAGE_FILE));
    public static final File bestTriedSendMessageFile = mkDir(new File(BEST_TRIED_SEND_MESSAGE_FILE));

    public static final File strictGenerateMessageFile = mkDir(new File(STRICT_GENERATE_MESSAGE_FILE));
    public static final File strictSendMessageFile = mkDir(new File(STRICT_SEND_MESSAGE_FILE));

    public static final File delayGenerateMessageFile = mkDir(new File(DELAY_GENERATE_MESSAGE_FILE));
    public static final File delaySendMessageFile = mkDir(new File(DELAY_SEND_MESSAGE_FILE));

    public static final String SHARED_BEST_TRIED_MESSAGE_SUBJECT = "shared.best.tried.subject";
    public static final String SHARED_STRICT_MESSAGE_SUBJECT = "shared.strict.subject";

    public static final String EXCLUSIVE_BEST_TRIED_MESSAGE_SUBJECT = "exclusive.best.tried.subject";
    public static final String EXCLUSIVE_STRICT_MESSAGE_SUBJECT = "exclusive.strict.subject";

    public static final String DELAY_MESSAGE_SUBJECT = "delay.message.subject";

    public static final String SHARED_CONSUMER_GROUP = "shared.consumer.group";
    public static final String EXCLUSIVE_CONSUMER_GROUP = "exclusive.consumer.group";

    public static final File sharedBestTriedConsumerMessageFile = mkDir(new File(SHARED_BEST_TRIED_CONSUMER_FILE));
    public static final File sharedStrictConsumerMessageFile = mkDir(new File(SHARED_STRICT_CONSUMER_FILE));
    public static final File exclusiveBestTriedConsumerMessageFile = mkDir(
            new File(EXCLUSIVE_BEST_TRIED_CONSUMER_FILE));
    public static final File exclusiveStrictConsumerMessageFile = mkDir(new File(EXCLUSIVE_STRICT_CONSUMER_FILE));

    private static File mkDir(File file) {
        File parentFile = file.getParentFile();
        if (!parentFile.exists()) {
            parentFile.mkdirs();
        }
        return file;
    }

    public static String getClientId() {
        return System.getProperty("qmq.client.id");
    }

    public static void setClientId(String clientId) {
        System.setProperty("qmq.client.id", clientId);
    }

    /**
     * 生成随机消息, 并将消息记录保存到文件中
     *
     * @param count 消息数量
     * @return 生成的消息列表
     */
    public static List<Message> generateMessages(MessageProducerProvider provider, String subject, int count) {
        List<Message> result = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            Message message = provider.generateMessage(subject);
            message.setProperty("idx_property", i);
            result.add(message);
        }

        return result;
    }

    public static void saveMessage(List<Message> messages, File file) throws IOException {
        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file)))) {
            for (Message message : messages) {
                saveMessage(message, writer);
            }
        }
    }

    public static void saveMessage(Message message, PrintWriter writer) {
        writer.println(serializeMessage(message));
    }

    public static List<Message> replayMessages(File file) throws IOException {
        List<Message> result = Lists.newArrayList();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String s;
            while ((s = reader.readLine()) != null) {
                Message message = deserializeMessage(s);
                result.add(message);
            }
        }
        return result;
    }

    /**
     * 检查 BEST_TRIED 策略的发送消息文件是否正确
     *
     * @param generateMessageFile 生成消息存储文件
     * @param sendMessageFile 发送成功消息存储文件
     */
    public static void checkBestTriedSendMessageFile(File generateMessageFile, File sendMessageFile)
            throws IOException {
        List<Message> generateMessages = replayMessages(generateMessageFile);
        List<Message> sendMessages = replayMessages(sendMessageFile);

        // 1. 生成的消息必须包括所有发送成功的消息, 但发送成功的消息, 不一定包含所有生成的消息
        Set<String> generateMessageIdSet = generateMessages.stream().map(Message::getMessageId)
                .collect(Collectors.toSet());
        Set<String> sendMessageIdSet = sendMessages.stream().map(Message::getMessageId).collect(Collectors.toSet());

        assertTrue(generateMessageIdSet.size() >= sendMessageIdSet.size());
        assertTrue(generateMessageIdSet.containsAll(sendMessageIdSet));

        // 2. 验证同一个 Broker 分区的消息 messageId 是按顺序的
        checkMessagesGroupOrder(sendMessages);
    }

    /**
     * 检查 STRICT 策略的发送消息文件是否正确
     *
     * @param generateMessageFile 生成消息存储文件
     * @param sendMessageFile 发送成功消息存储文件
     */
    public static void checkStrictSendMessageFile(File generateMessageFile, File sendMessageFile) throws IOException {
        List<Message> generateMessages = replayMessages(generateMessageFile);
        List<Message> sendMessages = replayMessages(sendMessageFile);

        // 1. 生成的消息必须数量和 msgId 与发送成功的消息相同, 但顺序可以不一样
        Set<String> generateMessageIdSet = generateMessages.stream().map(Message::getMessageId)
                .collect(Collectors.toSet());
        Set<String> sendMessageIdSet = sendMessages.stream().map(Message::getMessageId).collect(Collectors.toSet());

        assertEquals(generateMessageIdSet, sendMessageIdSet);

        // 2. 验证同一个 Broker 分区的消息 messageId 是按顺序的
        checkMessagesGroupOrder(sendMessages);
    }

    public static void checkSharedBestTriedConsumeMessageFile(File generateMessageFile, File consumeMessageFile)
            throws IOException {
        List<Message> generateMessages = replayMessages(generateMessageFile);
        List<Message> consumeMessages = replayMessages(consumeMessageFile);

        assertEquals(generateMessages.size(), consumeMessages.size());

        // 1. 因为消费没有出异常, 不存在最大重试失败的问题, 生成的消息必须数量和 msgId 与消费成功的消息相同, 但顺序可以不一样
        Set<String> generateMessageIdSet = generateMessages.stream().map(Message::getMessageId)
                .collect(Collectors.toSet());
        Set<String> consumeMessageIdSet = consumeMessages.stream().map(Message::getMessageId)
                .collect(Collectors.toSet());

        assertEquals(generateMessageIdSet, consumeMessageIdSet);
    }

    public static void checkSharedStrictConsumeMessageFile(File generateMessageFile, File consumeMessageFile)
            throws IOException {
        List<Message> generateMessages = replayMessages(generateMessageFile);
        List<Message> consumeMessages = replayMessages(consumeMessageFile);

        assertEquals(generateMessages.size(), consumeMessages.size());

        // 1. 因为消费没有出异常, 不存在最大重试失败的问题, 生成的消息必须数量和 msgId 与消费成功的消息相同, 但顺序可以不一样
        Set<String> generateMessageIdSet = generateMessages.stream().map(Message::getMessageId)
                .collect(Collectors.toSet());
        Set<String> consumeMessageIdSet = consumeMessages.stream().map(Message::getMessageId)
                .collect(Collectors.toSet());

        assertEquals(generateMessageIdSet, consumeMessageIdSet);
    }

    public static void checkExclusiveBestTriedConsumeMessageFile(File generateMessageFile, File consumeMessageFile)
            throws IOException {
        List<Message> generateMessages = replayMessages(generateMessageFile);
        List<Message> consumeMessages = replayMessages(consumeMessageFile);

        assertEquals(generateMessages.size(), consumeMessages.size());

        // 1. 因为消费没有出异常, 不存在最大重试失败的问题, 生成的消息必须数量和 msgId 与消费成功的消息相同, 且同分区的顺序必须一样
        Set<String> generateMessageIdSet = generateMessages.stream().map(Message::getMessageId)
                .collect(Collectors.toSet());
        Set<String> consumeMessageIdSet = consumeMessages.stream().map(Message::getMessageId)
                .collect(Collectors.toSet());

        assertEquals(generateMessageIdSet, consumeMessageIdSet);

        // 2. 检查同分区的消息顺序是否一致
        Map<String, List<Message>> orderKey2Message = groupOrderedMessage(consumeMessages);
        for (List<Message> messageList : orderKey2Message.values()) {
            Message lastMessage = null;
            for (Message message : messageList) {
                if (lastMessage == null) {
                    lastMessage = message;
                } else {
                    assertTrue(compareMessageId(message.getMessageId(), lastMessage.getMessageId()) > 0);
                }
            }
        }
    }

    public static void checkExclusiveBestTriedConsumeMessageExceptionFile(File generateMessageFile, File consumeMessageFile)
            throws IOException {
        List<Message> generateMessages = replayMessages(generateMessageFile);
        List<Message> consumeMessages = replayMessages(consumeMessageFile);

        assertEquals(generateMessages.size(), consumeMessages.size());

        // 1. 生成的消息必须数量和 msgId 与消费成功的消息相同, 虽然是顺序消息, 但是出异常以后使用 %retry% 机制会导致消息乱序
        Set<String> generateMessageIdSet = generateMessages.stream().map(Message::getMessageId)
                .collect(Collectors.toSet());
        Set<String> consumeMessageIdSet = consumeMessages.stream().map(Message::getMessageId)
                .collect(Collectors.toSet());

        assertEquals(generateMessageIdSet, consumeMessageIdSet);
    }

    public static void checkExclusiveStrictConsumeMessageFile(File generateMessageFile, File consumeMessageFile)
            throws IOException {
        List<Message> generateMessages = replayMessages(generateMessageFile);
        List<Message> consumeMessages = replayMessages(consumeMessageFile);

        assertEquals(generateMessages.size(), consumeMessages.size());

        // 1. 因为消费没有出异常, 不存在最大重试失败的问题, 生成的消息必须数量和 msgId 与消费成功的消息相同, 且同分区的顺序必须一样
        Set<String> generateMessageIdSet = generateMessages.stream().map(Message::getMessageId)
                .collect(Collectors.toSet());
        Set<String> consumeMessageIdSet = consumeMessages.stream().map(Message::getMessageId)
                .collect(Collectors.toSet());

        assertEquals(generateMessageIdSet, consumeMessageIdSet);

        // 2. 检查同分区的消息顺序是否一致
        Map<String, List<Message>> orderKey2Message = groupOrderedMessage(consumeMessages);
        for (List<Message> messageList : orderKey2Message.values()) {
            Message lastMessage = null;
            for (Message message : messageList) {
                if (lastMessage == null) {
                    lastMessage = message;
                } else {
                    assertTrue(compareMessageId(message.getMessageId(), lastMessage.getMessageId()) > 0);
                }
            }
        }
    }

    private static Map<String, List<Message>> groupOrderedMessage(List<Message> messages) {
        Map<String, List<Message>> map = Maps.newConcurrentMap();
        for (Message message : messages) {
            String orderKey = message.getOrderKey();
            List<Message> messageList = map.computeIfAbsent(orderKey, k -> Lists.newArrayList());
            messageList.add(message);
        }
        return map;
    }

    public static String getOrderKey(Message message) {
        return String.valueOf(message.getMessageId().hashCode() % 3);
    }

    private static void checkMessagesGroupOrder(List<Message> sendMessages) {
        Map<String, List<Message>> groupSendMessages = Maps.newConcurrentMap();
        for (Message sendMessage : sendMessages) {
            if(sendMessage.getOrderKey() == null) {
                return;
            }
            String key = sendMessage.getOrderKey();
            List<Message> messages = groupSendMessages.computeIfAbsent(key, k -> Lists.newArrayList());
            messages.add(sendMessage);
        }

        for (Entry<String, List<Message>> entry : groupSendMessages.entrySet()) {
            List<Message> messages = entry.getValue();
            String lastMessageId = null;
            for (Message message : messages) {
                if (lastMessageId == null) {
                    lastMessageId = message.getMessageId();
                } else {
                    String currentMessageId = message.getMessageId();
                    if (compareMessageId(currentMessageId, lastMessageId) <= 0) {
                        throw new IllegalStateException(
                                String.format("消息乱序, currentId %s lastId %s", currentMessageId, lastMessageId));
                    }
                }
            }
        }
    }

    private static final Splitter MID_SP = Splitter.on(".");

    private static int compareMessageId(String mid1, String mid2) {
        List<String> sp1s = MID_SP.splitToList(mid1);
        List<String> sp2s = MID_SP.splitToList(mid2);
        assertEquals(sp1s.size(), sp2s.size());
        for (int i = 0; i < sp1s.size(); i++) {
            long sp1Part = Long.valueOf(sp1s.get(i));
            long sp2Part = Long.valueOf(sp2s.get(i));
            if (sp1Part > sp2Part) {
                return 1;
            } else if (sp1Part < sp2Part) {
                return -1;
            }
        }
        return 0;
    }

    private static final String PROP_SEP = "||||";
    private static final Joiner msgJoiner = Joiner.on(PROP_SEP);
    private static final Splitter msgSplitter = Splitter.on(PROP_SEP);

    public static String serializeMessage(Message message) {
        return msgJoiner.join(new String[]{message.getSubject(), message.getMessageId(),
                JsonUtils.serialize(message.getAttrs())});
    }

    public static Message deserializeMessage(String s) {
        List<String> ss = msgSplitter.splitToList(s);
        String subject = ss.get(0);
        String messageId = ss.get(1);
        Map<Object, Object> props = JsonUtils.deSerialize(ss.get(2), Map.class);
        BaseMessage message = new BaseMessage(messageId, subject);
        for (Entry<Object, Object> entry : props.entrySet()) {
            String key = entry.getKey().toString();
            try {
                keys keys = BaseMessage.keys.valueOf(key);
                message.setProperty(keys, entry.getValue().toString());
            } catch (IllegalArgumentException e) {
                message.setProperty(key, entry.getValue().toString());
            }
        }
        return message;
    }
}
