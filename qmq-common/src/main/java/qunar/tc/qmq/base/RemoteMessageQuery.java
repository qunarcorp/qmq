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

package qunar.tc.qmq.base;

import java.io.Serializable;
import java.util.List;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-20 15:40
 */
public class RemoteMessageQuery implements Serializable {
    private static final long serialVersionUID = 8856153577326864895L;

    private String subject;
    private List<MessageKey> keys;

    public RemoteMessageQuery(String subject, List<MessageKey> keys) {
        this.subject = subject;
        this.keys = keys;
    }

    public String getSubject() {
        return subject;
    }

    public List<MessageKey> getKeys() {
        return keys;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public void setKeys(List<MessageKey> keys) {
        this.keys = keys;
    }

    @Override
    public String toString() {
        return "RemoteMessageQuery{" +
                "subject='" + subject + '\'' +
                ", keys=" + keys +
                '}';
    }

    public static class MessageKey {
        private long sequence;

        public MessageKey(long sequence) {
            this.sequence = sequence;
        }

        public void setSequence(long sequence) {
            this.sequence = sequence;
        }

        public long getSequence() {
            return sequence;
        }

        @Override
        public String toString() {
            return "MessageKey{" +
                    "sequence=" + sequence +
                    '}';
        }
    }
}
