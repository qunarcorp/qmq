package qunar.tc.qmq.backup.test;

import qunar.tc.qmq.backup.service.BackupKeyGenerator;
import qunar.tc.qmq.backup.service.impl.DbDicService;
import qunar.tc.qmq.backup.store.impl.DbDicDao;
import qunar.tc.qmq.backup.store.impl.HFileIndexStore;
import qunar.tc.qmq.store.MessageQueryIndex;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * @Classname BulkloadTest
 * @Description 测试，生成messagequeryindex并写入hfile中，再上传到hbase
 * @Date 16.6.21 4:33 下午
 * @Created by zhipeng.cai
 */
public class BulkloadTest {
    public static void main(String[] args) throws IOException {
        BackupKeyGenerator keyGenerator=new BackupKeyGenerator(new DbDicService(new DbDicDao(false),"%06d"));
        HFileIndexStore hFileIndexStore=new HFileIndexStore(keyGenerator);
        for(int i=0;i<100;i++){
            //生成messagequeryindex
            String messageId="messageid"+String.format("%08d",i);
            long createTime=System.currentTimeMillis();
            long sequence=(long)i;
            MessageQueryIndex index=new MessageQueryIndex("testsubject",messageId,createTime,sequence);
            //System.out.println(i+index.toString());

            hFileIndexStore.appendData(index);
        }
    }
}

/*
When using the bulkloader (LoadIncrementalHFiles, doBulkLoad) you can only add items that are "lexically ordered", ie. you need to make sure that the items you add are sorted by the row-id.
 */