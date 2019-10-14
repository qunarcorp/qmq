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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import jdk.jfr.events.FileReadEvent;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.BaseMessage.keys;
import qunar.tc.qmq.common.JsonUtils;
import qunar.tc.qmq.producer.MessageProducerProvider;

/**
 * @author zhenwei.liu
 * @since 2019-10-14
 */
public class MessageTestManager {

    private static final String CLASS_PATH_ROOT = MessageTestManager.class.getClassLoader().getResource("").getPath();
    private static final String SENDER_ROOT = CLASS_PATH_ROOT + File.separator + "sender";
    private static final String BEST_TRIED_GENERATE_MESSAGE_FILE =
            SENDER_ROOT + File.separator + "best_tried_generate_messages";
    private static final String BEST_TRIED_SEND_MESSAGE_FILE =
            SENDER_ROOT + File.separator + "best_tried_send_messages";
    private static final String STRICT_SEND_MESSAGE_FILE = SENDER_ROOT + File.separator + "strict_send_messages";

    public static final File bestTriedGenerateMessageFile = mkDir(new File(BEST_TRIED_GENERATE_MESSAGE_FILE));
    public static final File bestTriedSendMessageFile = mkDir(new File(BEST_TRIED_SEND_MESSAGE_FILE));

    public static final String BEST_TRIED_MESSAGE_SUBJECT = "best.tried.subject";

    private static File mkDir(File file) {
        File parentFile = file.getParentFile();
        if (!parentFile.exists()) {
            parentFile.mkdirs();
        }
        return file;
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
            String s = reader.readLine();
            Message message = deserializeMessage(s);
            result.add(message);
        }
        return result;
    }

    /**
     * 检查 BEST_TRIED 策略的发送消息文件是否正确
     *
     * @param generateMessageFile 生成消息存储文件
     * @param sendMessageFile 发送成功消息存储文件
     */
    public static void checkBestTriedSendMessageFile(File generateMessageFile, File sendMessageFile) throws IOException {
        List<Message> generateMessages = replayMessages(generateMessageFile);
        List<Message> sendMessages = replayMessages(sendMessageFile);

        // 1. 生成的消息必须包括所有发送成功的消息, 但发送成功的消息, 不一定包含所有生成的消息
        Set<String> generateMessageIdSet = generateMessages.stream().map(Message::getMessageId).collect(Collectors.toSet());
        Set<String> sendMessageIdSet = sendMessages.stream().map(Message::getMessageId).collect(Collectors.toSet());

        assertEquals(generateMessageIdSet, sendMessageIdSet);

        // 2. 验证同一个 Broker 分区的消息 messageId 是按顺序的
        Map<String, List<Message>> groupSendMessages = Maps.newConcurrentMap();
        for (Message sendMessage : sendMessages) {
            String key = sendMessage.getStringProperty(keys.qmq_partitionBroker.name()) + ":"  + sendMessage.getPartitionName();
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
                    assertTrue(currentMessageId.compareTo(lastMessageId) > 0);
                }
            }
        }
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
