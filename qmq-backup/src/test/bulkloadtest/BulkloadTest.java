package bulkloadtest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;
import qunar.tc.qmq.backup.service.BackupKeyGenerator;
import qunar.tc.qmq.backup.service.impl.DbDicService;
import qunar.tc.qmq.backup.store.impl.DbDicDao;
import qunar.tc.qmq.backup.store.impl.HFileIndexStore;
import qunar.tc.qmq.store.MessageQueryIndex;

import java.io.IOException;

/**
 * @Classname BulkloadTest
 * @Description 测试，生成messagequeryindex并写入hfile中，再上传到hbase
 * @Date 16.6.21 4:33 下午
 * @Created by zhipeng.cai
 */

public class BulkloadTest {

    @Test
    public void bulkload() throws IOException {
        BackupKeyGenerator keyGenerator = new BackupKeyGenerator(new DbDicService(new DbDicDao(false), "%06d"));
        Configuration conf = HBaseConfiguration.create();
        HFileIndexStore hFileIndexStore = new HFileIndexStore(keyGenerator, conf);
        for (int i = 0; i < 100; i++) {
            //生成messagequeryindex
            String messageId = "messageid" + String.format("%08d", i);
            long createTime = System.currentTimeMillis();
            long sequence = (long) i;
            MessageQueryIndex index = new MessageQueryIndex("testsubject", messageId, createTime, sequence);

            hFileIndexStore.appendData(index, null);
        }
    }
}
