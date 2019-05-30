package qunar.tc.qmq.backup.util;

import org.hbase.async.KeyValue;
import org.jboss.netty.util.CharsetUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class KeyValueListImpl implements KeyValueList<KeyValue> {

    private byte[] key;

    private Map<String, KeyValue> data = new LinkedHashMap<>();

    private List<KeyValue> keyValues;

    public KeyValueListImpl(List<KeyValue> keyValues) {
        if (keyValues != null && !keyValues.isEmpty()) {
            this.keyValues = keyValues;
            key = keyValues.get(0).key();
            for (KeyValue keyValue : keyValues) {
                data.put(new String(keyValue.qualifier(), CharsetUtil.UTF_8), keyValue);
            }
        }
    }

    @Override
    public List<KeyValue> getKeyValues() {
        return keyValues;
    }


    public void setKeyValues(ArrayList<KeyValue> keyValues) {
        this.keyValues = keyValues;
    }


    @Override
    public Map<String, KeyValue> getData() {
        return data;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public String getStringValue(String qualifier) {
        byte[] value = getValue(qualifier);
        return (value == null) ? null : new String(value, CharsetUtil.UTF_8);
    }

    public KeyValue getKeyValue(String qualifier) {
        return data.get(qualifier);
    }

    @Override
    public byte[] getValue(String qualifier) {
        KeyValue keyValue = getKeyValue(qualifier);
        return (keyValue == null) ? null : keyValue.value();
    }

    @Override
    public Long getTimestamp(String qualifier) {
        KeyValue keyValue = getKeyValue(qualifier);
        return keyValue == null ? null : keyValue.timestamp();
    }


    @Override
    public int size() {
        return data.size();
    }
}