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

package qunar.tc.qmq.backup.store.impl;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import qunar.tc.qmq.backup.store.KvStore;
import qunar.tc.qmq.metrics.Metrics;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-07 13:56
 */
public abstract class AbstractHBaseStore implements KvStore {
    public static final String CONTENT = "c";
    public static final String RECORDS = "r";

    public static final byte[] B_CONTENT = Bytes.UTF8(CONTENT);
    public static final byte[] B_RECORDS = Bytes.UTF8(RECORDS);

    public static final byte[] B_FAMILY = Bytes.UTF8("m");
    public static final byte[] R_FAMILY = Bytes.UTF8("i");

    public static final byte[][] B_MESSAGE_QUALIFIERS = new byte[][]{B_CONTENT};
    public static final byte[][] B_RECORD_QUALIFIERS = new byte[][]{B_RECORDS};

    protected final byte[] table;
    private final byte[] family;
    private final byte[][] qualifiers;

    AbstractHBaseStore(final byte[] table, final byte[] family, final byte[][] qualifiers) {
        this.table = table;
        this.family = family;
        this.qualifiers = qualifiers;
    }

    @Override
    public void batchSave(byte[][] key, byte[][][] value) {
        long currentTime = System.currentTimeMillis();
        try {
            doBatchSave(table, key, family, qualifiers, value);
        } finally {
            Metrics.timer("HBaseStore.Store.Timer").update(System.currentTimeMillis() - currentTime, TimeUnit.MILLISECONDS);
        }
    }

    protected abstract void doBatchSave(final byte[] table, final byte[][] keys, final byte[] family, byte[][] qualifiers, byte[][][] values);

    protected abstract <T, V> List<T> scan(byte[] table, String keyRegexp, String startKey, String stopKey, int maxNumRows, int maxVersions, byte[] family, byte[][] qualifiers, RowExtractor<T> rowExtractor) throws Exception;

    protected abstract <T> T get(byte[] table, byte[] key, byte[] family, byte[][] qualifiers, RowExtractor<T> rowExtractor) throws Exception;
}

interface RowExtractor<T> {
    T extractData(List<KeyValue> kvs);
}
