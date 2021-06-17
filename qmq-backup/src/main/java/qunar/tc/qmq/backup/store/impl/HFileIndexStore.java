package qunar.tc.qmq.backup.store.impl;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
import org.hbase.async.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import qunar.tc.qmq.backup.config.BackupConfig;
import qunar.tc.qmq.backup.config.DefaultBackupConfig;
import qunar.tc.qmq.backup.service.BackupKeyGenerator;
import qunar.tc.qmq.backup.service.impl.DbDicService;
import qunar.tc.qmq.backup.test.BulkloadTest;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.store.MessageQueryIndex;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.io.IOException;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import static qunar.tc.qmq.backup.service.DicService.SIX_DIGIT_FORMAT_PATTERN;
import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_ARRAY;

/**
 * @Classname HFileIndexStore
 * @Description 将数据写入hfile并用bulkload上传至hbase
 * @Date 16.6.21 2:11 下午
 * @Created by zhipeng.cai
 */
public class HFileIndexStore {
    private final Configuration conf;
    private final Configuration tempConf;
    private final byte[] FAMILY_NAME;
    private final byte[] COLOMU_NAME;
    private final String HFILE_DIR_STRING;
    private final Path HFILE_DIR;
    private final Path FAMILY_DIR;
    private final int BLOCK_SIZE;
    private final HFile.Writer writer;
    private final DynamicConfig skipBackSubjects;
    private final BackupKeyGenerator keyGenerator;
    private final BackupConfig config;
    private final String brokerGroup;
    private final byte[] brokerGroupBytes;
    private final int brokerGroupLength;
    private final String TABLE_NAME="test";
    private Map<byte[],KeyValue> map = new TreeMap<>(new ByteArrayComparator());

    public HFileIndexStore() throws IOException {
        this.conf= HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","common11.w.hbase.dev.bj1.wormpex.com,common12.w.hbase.dev.bj1.wormpex.com,common13.w.hbase.dev.bj1.wormpex.com,common14.w.hbase.dev.bj1.wormpex.com,common15.w.hbase.dev.bj1.wormpex.com");
        conf.set("hbase.zookeeper.znode.parent","/hbase-dev");
        //这里先设置hdfs为本地的方便查看hfile文件
        conf.set("fs.defaultFS","hdfs://10.1.24.53:9000");
        FAMILY_NAME= Bytes.UTF8("fm1");//列簇名
        COLOMU_NAME=Bytes.UTF8("col");//列名
        HFILE_DIR_STRING="/tmp/message";
        HFILE_DIR=new Path(HFILE_DIR_STRING);
        FAMILY_DIR=new Path("/tmp/message/fm1/outputhfile");
        //FAMILY_DIR=new Path(HFILE_DIR_STRING, "/fm1/outputhfile");
        BLOCK_SIZE=64000;
        tempConf=new Configuration(conf);
        tempConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 1.0f);
        HFileContext fileContext = new HFileContext();
        //fileContext.setCompression(Compression.Algorithm.NONE);
        writer= HFile.getWriterFactory(conf,new CacheConfig(tempConf))
                .withPath(FileSystem.get(conf),FAMILY_DIR)
                .withFileContext(fileContext).create();
        this.skipBackSubjects = DynamicConfigLoader.load("skip_backup.properties", false);
        //this.config = DynamicConfigLoader.load("backup.properties");
        this.config=new DefaultBackupConfig(DynamicConfigLoader.load("backup.properties",false));
        //this.brokerGroup=this.config.getBrokerGroup();
        this.brokerGroup="brokergroup";
        this.brokerGroupBytes = Bytes.UTF8(brokerGroup);
        this.brokerGroupLength = this.brokerGroupBytes.length;
        this.keyGenerator=new BackupKeyGenerator(new DbDicService(new DbDicDao(false),SIX_DIGIT_FORMAT_PATTERN));
    }

    public void appendData(MessageQueryIndex index){
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

        long currentTime=System.currentTimeMillis();
        KeyValue kv=new KeyValue(key,FAMILY_NAME,COLOMU_NAME,currentTime,value);
        //先添加到treemap中
        map.put(key,kv);
        if(map.size()>=100){
            try {
                writeToHfile();
                map.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void writeToHfile() throws IOException {
        for (Map.Entry<byte[],KeyValue> entry : map.entrySet()) {
            System.out.println(entry.getValue());
            writer.append(entry.getValue());
        }
        writer.close();
        System.out.println("write hfile success......");

        //用bulkload上传至hbase

        Connection conn= ConnectionFactory.createConnection(conf);
        Table htable= conn.getTable(TableName.valueOf(TABLE_NAME));
        Admin admin= conn.getAdmin();
        try {
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            //新版本里改用这个了
            //BulkLoadHFilesTool loader=new BulkLoadHFilesTool(conf);
            //bulk load开始时间
            long startTime = System.currentTimeMillis();
            //loader.doBulkLoad(FAMILY_DIR, (HTable) htable);
            loader.doBulkLoad(HFILE_DIR,admin,htable,conn.getRegionLocator(TableName.valueOf(TABLE_NAME)));
            long endTime = System.currentTimeMillis();
            System.out.println("bulk load 结束........");
            long runTime=endTime-startTime;
            System.out.println("bulk load 所需时间="+(runTime/60000)+"分"+(runTime/1000)+"秒");
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

    //byte[]比较器
    static class ByteArrayComparator implements Comparator<byte[]> {
        public int compare(byte[] o1, byte[] o2) {
            int offset1 = 0;
            int offset2 = 0;
            int length1 = o1.length;
            int length2 = o2.length;
            int end1 = offset1 + length1;
            int end2 = offset2 + length2;
            for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
                int a = (o1[i] & 0xff);
                int b = (o2[j] & 0xff);
                if (a != b) {
                    return a - b;
                }
            }
            return length1 - length2;
        }
    }
}
