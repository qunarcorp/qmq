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

package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.TagType;
import qunar.tc.qmq.consumer.register.RegistParam;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.Collections;
import java.util.Set;

/**
 * @author yiqun.fan create on 17-11-2.
 */
class ConsumeParam {
    private final String subject;
    private final String group;
    private final String originSubject;
    private final String retrySubject;
    private final String consumerId;
    private final boolean isBroadcast;
    private volatile boolean isConsumeMostOnce;
    private volatile TagType tagType;
    private volatile Set<String> tags;

    public ConsumeParam(String subject, String group, RegistParam param) {
        this(subject, group, param.isBroadcast(), param.getSubscribeParam().isConsumeMostOnce(), param.getSubscribeParam().getTagType(), param.getSubscribeParam().getTags(), param.getClientId());
    }

    public ConsumeParam(String subject, String group, boolean isBroadcast, boolean isConsumeMostOnce, String clientId) {
        this(subject, group, isBroadcast, isConsumeMostOnce, TagType.NO_TAG, Collections.EMPTY_SET, clientId);
    }

    public ConsumeParam(String subject, String group, boolean isBroadcast, boolean isConsumeMostOnce, TagType tagType, Set<String> tags, String clientId) {
        this.subject = subject;
        this.group = group;
        this.originSubject = RetrySubjectUtils.isRetrySubject(subject) ? RetrySubjectUtils.getRealSubject(subject) : subject;
        this.retrySubject = RetrySubjectUtils.buildRetrySubject(originSubject, group);
        this.consumerId = clientId;
        this.isBroadcast = isBroadcast;
        this.isConsumeMostOnce = isConsumeMostOnce;
        this.tagType = tagType;
        this.tags = tags;
    }

    public String getSubject() {
        return subject;
    }

    public String getGroup() {
        return group;
    }

    public String getOriginSubject() {
        return originSubject;
    }

    public String getRetrySubject() {
        return retrySubject;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }

    public boolean isConsumeMostOnce() {
        return isConsumeMostOnce;
    }

    public void setConsumeMostOnce(boolean consumeMostOnce) {
        isConsumeMostOnce = consumeMostOnce;
    }

    public TagType getTagType() {
        return tagType;
    }

    public void setTagType(TagType tagType) {
        this.tagType = tagType;
    }

    public Set<String> getTags() {
        return tags;
    }

    public void setTags(Set<String> tags) {
        this.tags = tags;
    }
}
