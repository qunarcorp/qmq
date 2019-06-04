package qunar.tc.qmq.backup.util;

import org.hbase.async.KeyValue;

import java.util.List;
import java.util.Map;

public interface KeyValueList<T> {

    String getStringValue(String qualifier);

    byte[] getValue(String qualifier);

    Long getTimestamp(String qualifier);

    byte[] getKey();

    Map<String, T> getData();

    int size();

    List<KeyValue> getKeyValues();
}
