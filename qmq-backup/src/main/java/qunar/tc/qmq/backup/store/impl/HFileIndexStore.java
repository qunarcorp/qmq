package qunar.tc.qmq.backup.store.impl;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.hbase.async.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.config.BackupConfig;
import qunar.tc.qmq.backup.config.DefaultBackupConfig;
import qunar.tc.qmq.backup.service.BackupKeyGenerator;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.store.MessageQueryIndex;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import static qunar.tc.qmq.backup.config.DefaultBackupConfig.*;
import static qunar.tc.qmq.backup.store.impl.AbstractHBaseStore.B_FAMILY;
import static qunar.tc.qmq.backup.store.impl.AbstractHBaseStore.B_MESSAGE_QUALIFIERS;
import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_ARRAY;

/**
 * @Classname HFileIndexStore
 * @Description 将数据写入hfile并用bulkload上传至hbase
 * @Date 16.6.21 2:11 下午
 * @Created by zhipeng.cai
 */
public class HFileIndexStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(HFileIndexStore.class);
    private final BackupConfig config;
    private final DynamicConfig hbaseConfig;
    private final DynamicConfig skipBackSubjects;
    private final BackupKeyGenerator keyGenerator;
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
    private MessageQueryIndex lastIndex;
    private HFile.Writer writer;
    private Map<byte[], KeyValue> map = new TreeMap<>(new org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator());

    public HFileIndexStore(BackupKeyGenerator keyGenerator) {
        this.config = new DefaultBackupConfig(DynamicConfigLoader.load("backup.properties", false));
        this.brokerGroup = BrokerConfig.getBrokerName();
        //this.brokerGroup=this.config.getBrokerGroup()==null? "brokergroup":this.config.getBrokerGroup();//本机调试用
        this.brokerGroupBytes = Bytes.UTF8(brokerGroup);
        this.brokerGroupLength = this.brokerGroupBytes.length;
        this.keyGenerator = keyGenerator;
        this.skipBackSubjects = DynamicConfigLoader.load("skip_backup.properties", false);
        this.hbaseConfig = DynamicConfigLoader.load(DEFAULT_HBASE_CONFIG_FILE, false);
        this.conf = HBaseConfiguration.create();
        this.conf.addResource("core-site.xml");
        this.conf.addResource("hdfs-site.xml");
        this.conf.set("hbase.zookeeper.quorum", hbaseConfig.getString("hbase.zookeeper.quorum", "localhost"));
        this.conf.set("zookeeper.znode.parent", hbaseConfig.getString("hbase.zookeeper.znode.parent", "/hbase"));
        this.TABLE_NAME = "qmq_backup3";
        //this.TABLE_NAME=this.config.getDynamicConfig().getString(HBASE_MESSAGE_INDEX_TABLE_CONFIG_KEY, DEFAULT_HBASE_MESSAGE_INDEX_TABLE);
        this.FAMILY_NAME = B_FAMILY;
        this.QUALIFIERS_NAME = B_MESSAGE_QUALIFIERS[0];//列名 TODO 这里可能要改
        this.HFILE_PARENT_PARENT_DIR = new Path("/tmp/trace");
        this.HFILE_PATH = new Path("/tmp/trace/" + new String(FAMILY_NAME) + "/hfile");
        this.tempConf = new Configuration(this.conf);
        this.tempConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 1.0f);
    }

    public void appendData(MessageQueryIndex index, Consumer<MessageQueryIndex> consumer) {
        lastIndex = index;
        String subject = index.getSubject();
        String realSubject = RetrySubjectUtils.getRealSubject(subject);
        if (skipBackup(realSubject)) {
            return;
        }
        monitorBackupIndexQps(subject);
        String subjectKey = realSubject;
        String consumerGroup = null;
        if (RetrySubjectUtils.isRetrySubject(subject)) {
            subjectKey = RetrySubjectUtils.buildRetrySubject(realSubject);
            consumerGroup = RetrySubjectUtils.getConsumerGroup(subject);
        }
        final byte[] key = keyGenerator.generateMessageKey(subjectKey, new Date(index.getCreateTime()), index.getMessageId(), brokerGroup, consumerGroup, index.getSequence());
        final String messageId = index.getMessageId();
        final byte[] messageIdBytes = Bytes.UTF8(messageId);

        final byte[] value = new byte[20 + brokerGroupLength + messageIdBytes.length];
        Bytes.setLong(value, index.getSequence(), 0);
        Bytes.setLong(value, index.getCreateTime(), 8);
        Bytes.setInt(value, brokerGroupLength, 16);
        System.arraycopy(brokerGroupBytes, 0, value, 20, brokerGroupLength);
        System.arraycopy(messageIdBytes, 0, value, 20 + brokerGroupLength, messageIdBytes.length);

        long currentTime = System.currentTimeMillis();
        KeyValue kv = new KeyValue(key, FAMILY_NAME, QUALIFIERS_NAME, currentTime, value);
        //先添加到treemap中
        map.put(key, kv);
        if (map.size() >= 10000) {
            try {
                //bulk load开始时间
                long startTime = System.currentTimeMillis();
                writeToHfile();
                bulkLoad();
                long endTime = System.currentTimeMillis();
                long runTime = endTime - startTime;
                LOGGER.info("bulk load 结束........");
                LOGGER.info("bulk load 所需时间=" + (runTime / 60000) + "分" + (runTime / 1000) + "秒");
                map.clear();
                if (consumer != null) consumer.accept(lastIndex);
            } catch (IOException e) {
                e.printStackTrace();
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
            LOGGER.info("写入HFile成功");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            writer.close();
        }
    }

    private void bulkLoad() throws IOException {
        //用bulkload上传至hbase
        Connection conn = ConnectionFactory.createConnection(conf);
        Table htable = conn.getTable(TableName.valueOf(TABLE_NAME));
        Admin admin = conn.getAdmin();
        try {
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            //新版本(2.x.y)里改用这个了
            //BulkLoadHFilesTool loader=new BulkLoadHFilesTool(conf);
            loader.doBulkLoad(HFILE_PARENT_PARENT_DIR, admin, htable, conn.getRegionLocator(TableName.valueOf(TABLE_NAME)));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (htable != null) {
                htable.close();
            }
        }
    }

    private boolean skipBackup(String subject) {
        return skipBackSubjects.getBoolean(subject, false);
    }

    private static void monitorBackupIndexQps(String subject) {
        Metrics.meter("backup.message.index.qps", SUBJECT_ARRAY, new String[]{subject}).mark();
    }
}
