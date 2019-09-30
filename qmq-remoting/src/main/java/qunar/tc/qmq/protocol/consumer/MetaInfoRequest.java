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

package qunar.tc.qmq.protocol.consumer;

import com.google.common.base.Strings;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.base.OnOfflineState;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author yiqun.fan create on 17-8-31.
 */
public class MetaInfoRequest {
    private static final String SUBJECT = "subject";
    private static final String CLIENT_TYPE_CODE = "clientTypeCode";
    private static final String APP_CODE = "appCode";
    private static final String CLIENT_ID = "clientId";
    private static final String CONSUMER_GROUP = "consumerGroup";
    private static final String REQUEST_TYPE = "requestType";
    private static final String ONLINE_STATE = "onlineState";
    private static final String IS_ORDERED = "isOrdered";
    private static final String IS_BROADCAST = "isBroadcast";

    private final Map<String, String> attrs;

    public MetaInfoRequest(
            String subject,
            String consumerGroup,
            int clientTypeCode,
            String appCode,
            String clientId,
            ClientRequestType requestType,
            boolean isBroadcast,
            boolean isOrdered
    ) {
        this.attrs = new HashMap<>();
        setStringValue(SUBJECT, subject);
        setStringValue(CONSUMER_GROUP, consumerGroup);
        setIntValue(CLIENT_TYPE_CODE, clientTypeCode);
        setStringValue(APP_CODE, appCode);
        setStringValue(CLIENT_ID, clientId);
        setIntValue(REQUEST_TYPE, requestType.getCode());
        // consumer 特有参数
        setBooleanValue(IS_ORDERED, isOrdered);
        setBooleanValue(IS_BROADCAST, isBroadcast);
    }

    public MetaInfoRequest(Map<String, String> attrs) {
        this.attrs = new HashMap<>(attrs);
    }

    Map<String, String> getAttrs() {
        return attrs;
    }

    public boolean isBroadcast() {
        return getBooleanValue(IS_BROADCAST);
    }

    public boolean isOrdered() {
        return getBooleanValue(IS_ORDERED);
    }

    public String getSubject() {
        return Strings.nullToEmpty(attrs.get(SUBJECT));
    }

    public int getClientTypeCode() {
        return getIntValue(CLIENT_TYPE_CODE, ClientType.OTHER.getCode());
    }

    public String getAppCode() {
        return getStringValue(APP_CODE);
    }

    public String getClientId() {
        return Strings.nullToEmpty(attrs.get(CLIENT_ID));
    }

    public String getConsumerGroup() {
        return Strings.nullToEmpty(attrs.get(CONSUMER_GROUP));
    }

    public int getRequestType() {
        return getIntValue(REQUEST_TYPE, ClientRequestType.ONLINE.getCode());
    }

    public void setRequestType(int typeCode) {
        setIntValue(REQUEST_TYPE, typeCode);
    }

    public OnOfflineState getOnlineState() {
        return OnOfflineState.valueOf(getStringValue(ONLINE_STATE));
    }

    public void setOnlineState(OnOfflineState state) {
        setStringValue(ONLINE_STATE, state.name());
    }

    private void setIntValue(String attrName, int value) {
        attrs.put(attrName, Integer.toString(value));
    }

    private int getIntValue(String attrName, int defaultValue) {
        try {
            return Integer.parseInt(attrs.get(attrName));
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private void setBooleanValue(String key, boolean val) {
        attrs.put(key, Boolean.toString(val));
    }

    private boolean getBooleanValue(String key) {
        return Boolean.valueOf(attrs.get(key));
    }

    private void setStringValue(String attrName, String value) {
        attrs.put(attrName, Strings.nullToEmpty(value));
    }

    private String getStringValue(String attrName) {
        return Strings.nullToEmpty(attrs.get(attrName));
    }

    @Override
    public String toString() {
        return "MetaInfoRequest{" + "attrs='" + attrs + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetaInfoRequest)) return false;

        MetaInfoRequest that = (MetaInfoRequest) o;

        return Objects.equals(attrs, that.attrs);
    }

    @Override
    public int hashCode() {
        return attrs.hashCode();
    }
}
