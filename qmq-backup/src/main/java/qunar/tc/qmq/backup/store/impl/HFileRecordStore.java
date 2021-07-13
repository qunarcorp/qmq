package qunar.tc.qmq.backup.store.impl;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.hbase.async.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.ActionEnum;
import qunar.tc.qmq.backup.base.ActionRecord;
import qunar.tc.qmq.backup.base.BackupMessage;
import qunar.tc.qmq.backup.config.BackupConfig;
import qunar.tc.qmq.backup.config.DefaultBackupConfig;
import qunar.tc.qmq.backup.service.BackupKeyGenerator;
import qunar.tc.qmq.backup.store.RocksDBStore;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.store.Action;
import qunar.tc.qmq.store.MessageQueryIndex;
import qunar.tc.qmq.store.action.PullAction;
import qunar.tc.qmq.store.action.RangeAckAction;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static qunar.tc.qmq.backup.config.DefaultBackupConfig.*;
import static qunar.tc.qmq.backup.store.impl.AbstractHBaseStore.R_FAMILY;
import static qunar.tc.qmq.backup.store.impl.AbstractHBaseStore.B_RECORD_QUALIFIERS;
import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_ARRAY;
import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_GROUP_ARRAY;

/**
 * @Classname HFileRecordStore
 * @Description 将消息轨迹数据写入hfile并用bulkload上传至hbase
 * @Date 12.7.21 4:25 下午
 * @Created by zhipeng.cai
 */
public class HFileRecordStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(HFileRecordStore.class);

    protected static final String[] TYPE_ARRAY = new String[]{"type"};
    protected static final String[] RECORD_TYPE = new String[]{"record"};

    private final BackupConfig config;
    private final DynamicConfig hbaseConfig;
    private final DynamicConfig skipBackSubjects;
    private final BackupKeyGenerator keyGenerator;
    private final RocksDBStore rocksDBStore;
    private final String brokerGroup;
    private final byte[] brokerGroupBytes;
    private final int brokerGroupLength;
    private final Configuration conf;
    private final Configuration tempConf;
    private final String TABLE_NAME;
    private final byte[] FAMILY_NAME;
    private final byte[] QUALIFIERS_NAME;
    private final Path HFILE_PARENT_PARENT_DIR;
    private final Path HFILE_PATH;
    private final int MESSAGE_SIZE_PER_HFILE;
    private MessageQueryIndex lastIndex;//TODO
    private HFile.Writer writer;
    private Map<byte[], KeyValue> map = new TreeMap<>(new org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator());

    public HFileRecordStore(BackupKeyGenerator keyGenerator, RocksDBStore rocksDBStore) {
        this.config = new DefaultBackupConfig(DynamicConfigLoader.load("backup.properties", false));
        this.brokerGroup = BrokerConfig.getBrokerName();
        this.brokerGroupBytes = Bytes.UTF8(brokerGroup);
        this.brokerGroupLength = this.brokerGroupBytes.length;
        this.keyGenerator = keyGenerator;
        this.rocksDBStore = rocksDBStore;
        this.skipBackSubjects = DynamicConfigLoader.load("skip_backup.properties", false);
        this.hbaseConfig = DynamicConfigLoader.load(DEFAULT_HBASE_CONFIG_FILE, false);
        this.conf = HBaseConfiguration.create();
        this.conf.addResource("core-site.xml");
        this.conf.addResource("hdfs-site.xml");
        this.conf.set("hbase.zookeeper.quorum", hbaseConfig.getString("hbase.zookeeper.quorum", "localhost"));
        this.conf.set("zookeeper.znode.parent", hbaseConfig.getString("hbase.zookeeper.znode.parent", "/hbase"));
        this.conf.set("hbase.bulkload.retries.retryOnIOException","true");
        this.conf.setBoolean("mapreduce.map.speculative", false);
        this.conf.setBoolean("mapreduce.reduce.speculative", false);
        this.TABLE_NAME = this.config.getDynamicConfig().getString(HBASE_RECORD_TABLE_CONFIG_KEY, DEFAULT_HBASE_RECORD_TABLE);
        this.FAMILY_NAME = R_FAMILY;
        this.QUALIFIERS_NAME = B_RECORD_QUALIFIERS[0];//列名
        this.HFILE_PARENT_PARENT_DIR = new Path("/tmp/record");
        this.HFILE_PATH = new Path("/tmp/record/" + new String(FAMILY_NAME) + "/hfile");
        this.tempConf = new Configuration(this.conf);
        this.tempConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 1.0f);
        this.MESSAGE_SIZE_PER_HFILE = this.config.getDynamicConfig().getInt(MESSAGE_SIZE_PER_HFILE_CONFIG_KEY, DEFAULT_MESSAGE_SIZE_PER_HFILE);
    }

    public void appendData(ActionRecord record, Consumer<MessageQueryIndex> consumer) {
        if (!config.getDynamicConfig().getBoolean(ENABLE_RECORD_CONFIG_KEY, true)) return;
        List<BackupMessage> messages = processRecord(record);
        for(BackupMessage message:messages){
            String subject = message.getSubject();
            monitorBackupActionQps(subject);

            String realSubject = RetrySubjectUtils.getRealSubject(subject);
            if (RetrySubjectUtils.isRetrySubject(subject)) {
                message.setSubject(RetrySubjectUtils.buildRetrySubject(realSubject));
            } else if (RetrySubjectUtils.isDeadRetrySubject(subject)) {
                message.setSubject(RetrySubjectUtils.buildDeadRetrySubject(realSubject));
            }
            final String consumerGroup = message.getConsumerGroup();
            final byte[] key = keyGenerator.generateRecordKey(message.getSubject(), message.getSequence(), brokerGroup, consumerGroup, Bytes.UTF8(Byte.toString(message.getAction())));
            final String consumerId = message.getConsumerId();
            final byte[] consumerIdBytes = Bytes.UTF8(consumerId);
            final int consumerIdBytesLength = consumerIdBytes.length;
            final byte[] consumerGroupBytes = Bytes.UTF8(consumerGroup);
            final int consumerGroupLength = consumerGroupBytes.length;
            final long timestamp = message.getTimestamp();
            byte[] value = new byte[12 + consumerIdBytesLength + consumerGroupLength];
            Bytes.setLong(value, timestamp, 0);
            Bytes.setShort(value, (short) consumerIdBytesLength, 8);
            System.arraycopy(consumerIdBytes, 0, value, 10, consumerIdBytesLength);
            Bytes.setShort(value, (short) consumerGroupLength, 10 + consumerIdBytesLength);
            System.arraycopy(consumerGroupBytes, 0, value, 12 + consumerIdBytesLength, consumerGroupLength);

            long currentTime = System.currentTimeMillis();
            KeyValue kv = new KeyValue(key, FAMILY_NAME, QUALIFIERS_NAME, currentTime, value);
            LOGGER.info("消息轨迹主题 subject:" + message.getSubject() + "   key:" + new String(key));
            map.put(key, kv);
        }

        if (map.size() >= MESSAGE_SIZE_PER_HFILE) {
            //bulk load开始时间
            long startTime = System.currentTimeMillis();
            try {
                writeToHfile();
                bulkLoad();
                map.clear();
            } catch (IOException e) {
                LOGGER.error("Record Bulk Load fail", e);
            } finally {
                Metrics.timer("Record.Bulkload.Timer", TYPE_ARRAY, RECORD_TYPE).update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void writeToHfile() throws IOException {
        HFileContext fileContext = new HFileContext();
        try {
            writer = HFile.getWriterFactory(conf, new CacheConfig(tempConf))
                    .withPath(FileSystem.get(conf), HFILE_PATH)
                    .withFileContext(fileContext).create();
            for (Map.Entry<byte[], KeyValue> entry : map.entrySet()) {
                writer.append(entry.getValue());
            }
            LOGGER.info("Record Write to HFile successfully");
        } catch (IOException e) {
            LOGGER.error("Record Write to HFile fail", e);
        } finally {
            writer.close();
        }
    }

    private void bulkLoad() throws IOException {
        //用bulkload上传至hbase
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Table htable = conn.getTable(TableName.valueOf(TABLE_NAME));
             Admin admin = conn.getAdmin();) {
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            //新版本(2.x.y)里改用这个了
            //BulkLoadHFilesTool loader=new BulkLoadHFilesTool(conf);
            loader.doBulkLoad(HFILE_PARENT_PARENT_DIR, admin, htable, conn.getRegionLocator(TableName.valueOf(TABLE_NAME)));
            LOGGER.info("Record Bulk Load to HBase successfully");
        } catch (Exception e) {
            LOGGER.error("Record Bulk Load to HBase fail", e);
        }
    }

    private List<BackupMessage> processRecord(ActionRecord record) {
        final List<BackupMessage> batch = Lists.newArrayList();
            if (record.getAction() instanceof PullAction) {
                onPullAction(record, batch);
            } else if (record.getAction() instanceof RangeAckAction) {
                onAckAction(record, batch);
            }
        return batch;
    }

    private void onAckAction(ActionRecord record, List<BackupMessage> batch) {
        final RangeAckAction ackAction = (RangeAckAction) record.getAction();
        final String prefix = getActionPrefix(ackAction);
        String subject = RetrySubjectUtils.getRealSubject(ackAction.subject());
        for (long ackLogOffset = ackAction.getFirstSequence(); ackLogOffset <= ackAction.getLastSequence(); ackLogOffset++) {
            monitorAckAction(subject, ackAction.group());
            final Optional<String> consumerLogSequenceOptional = rocksDBStore.get(prefix + "$" + ackLogOffset);
            if (!consumerLogSequenceOptional.isPresent()) {
                monitorMissConsumerLogSeq(subject, ackAction.group());
                continue;
            }
            long consumerLogSequence = Long.parseLong(consumerLogSequenceOptional.get());
            final BackupMessage message = generateBaseMessage(ackAction, consumerLogSequence);
            message.setAction(ActionEnum.ACK.getCode());
            batch.add(message);
        }
    }

    private void onPullAction(ActionRecord record, List<BackupMessage> batch) {
        final PullAction pullAction = (PullAction) record.getAction();
        storePullLogSequence2ConsumerLogSequence(pullAction);
        createBackupMessagesForPullAction(pullAction, batch);
    }

    private void createBackupMessagesForPullAction(final PullAction pullAction, final List<BackupMessage> batch) {
        final String subject = RetrySubjectUtils.getRealSubject(pullAction.subject());
        for (long pullLogOffset = pullAction.getFirstSequence(); pullLogOffset <= pullAction.getLastSequence(); pullLogOffset++) {
            monitorPullAction(subject, pullAction.group());
            final Long consumerLogSequence = getConsumerLogSequence(pullAction, pullLogOffset);
            final BackupMessage message = generateBaseMessage(pullAction, consumerLogSequence);
            message.setAction(ActionEnum.PULL.getCode());
            batch.add(message);
        }
    }

    private Long getConsumerLogSequence(PullAction action, long pullLogOffset) {
        return action.getFirstMessageSequence() + (pullLogOffset - action.getFirstSequence());
    }

    private void storePullLogSequence2ConsumerLogSequence(PullAction pullAction) {
        final String prefix = getActionPrefix(pullAction);
        for (long i = pullAction.getFirstSequence(); i <= pullAction.getLastSequence(); ++i) {
            final String key = prefix + "$" + i;
            final String messageSequence = Long.toString(pullAction.getFirstMessageSequence() + (i - pullAction.getFirstSequence()));
            rocksDBStore.put(key, messageSequence);
        }
    }

    private BackupMessage generateBaseMessage(Action action, long sequence) {
        final BackupMessage message = new BackupMessage();
        message.setSubject(action.subject());
        message.setConsumerGroup(action.group());
        message.setConsumerId(action.consumerId());
        message.setTimestamp(action.timestamp());
        message.setSequence(sequence);
        return message;
    }

    private String getActionPrefix(final Action action) {
        return action.subject() + "$" + action.group() + "$" + action.consumerId();
    }

    private boolean skipBackup(String subject) {
        return skipBackSubjects.getBoolean(subject, false);
    }

    private static void monitorAckAction(String subject, String group) {
        Metrics.counter("on_ack_action", SUBJECT_GROUP_ARRAY, new String[]{subject, group}).inc();
    }

    private static void monitorMissConsumerLogSeq(String subject, String group) {
        Metrics.meter("AckAction.ConsumerLogSeq.Miss", SUBJECT_GROUP_ARRAY, new String[]{subject, group}).mark();
    }

    private static void monitorPullAction(String subject, String group) {
        Metrics.counter("on_pull_action", SUBJECT_GROUP_ARRAY, new String[]{subject, group}).inc();
    }

    private static void monitorBackupActionQps(String subject) {
        Metrics.meter("backup.action.qps", SUBJECT_ARRAY, new String[]{subject}).mark();
    }
}
