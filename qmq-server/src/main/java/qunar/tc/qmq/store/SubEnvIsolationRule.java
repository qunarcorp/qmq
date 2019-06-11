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

package qunar.tc.qmq.store;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author keli.wang
 * @since 2019-01-04
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SubEnvIsolationRule {
    private String subject;
    private String subjectEnv;
    private String subjectSubEnv;

    private String group;
    private String groupEnv;
    private String groupSubEnv;

    public String getSubject() {
        return subject;
    }

    public void setSubject(final String subject) {
        this.subject = subject;
    }

    public String getSubjectEnv() {
        return subjectEnv;
    }

    public void setSubjectEnv(final String subjectEnv) {
        this.subjectEnv = subjectEnv;
    }

    public String getSubjectSubEnv() {
        return subjectSubEnv;
    }

    public void setSubjectSubEnv(final String subjectSubEnv) {
        this.subjectSubEnv = subjectSubEnv;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(final String group) {
        this.group = group;
    }

    public String getGroupEnv() {
        return groupEnv;
    }

    public void setGroupEnv(final String groupEnv) {
        this.groupEnv = groupEnv;
    }

    public String getGroupSubEnv() {
        return groupSubEnv;
    }

    public void setGroupSubEnv(final String groupSubEnv) {
        this.groupSubEnv = groupSubEnv;
    }

    @Override
    public String toString() {
        return "SubEnvIsolationRule{" +
                "subject='" + subject + '\'' +
                ", subjectEnv='" + subjectEnv + '\'' +
                ", subjectSubEnv='" + subjectSubEnv + '\'' +
                ", group='" + group + '\'' +
                ", groupEnv='" + groupEnv + '\'' +
                ", groupSubEnv='" + groupSubEnv + '\'' +
                '}';
    }
}
