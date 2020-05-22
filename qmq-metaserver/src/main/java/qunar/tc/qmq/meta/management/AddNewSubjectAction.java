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

package qunar.tc.qmq.meta.management;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.springframework.dao.DuplicateKeyException;
import qunar.tc.qmq.meta.model.SubjectInfo;
import qunar.tc.qmq.meta.store.Store;

import javax.servlet.http.HttpServletRequest;

/**
 * @author keli.wang
 * @since 2018/6/19
 */
public class AddNewSubjectAction implements MetaManagementAction {
    private final Store store;

    public AddNewSubjectAction(final Store store) {
        this.store = store;
    }

    @Override
    public Object handleAction(final HttpServletRequest req) {
        final String subject = req.getParameter("subject");
        final String tag = req.getParameter("tag");

        if (Strings.isNullOrEmpty(subject) || Strings.isNullOrEmpty(tag)) {
            return ImmutableMap.of(
                    "error", "必须同时提供 subject 和 tag 两个参数"
            );
        }

        final SubjectInfo subjectInfo = store.getSubjectInfo(subject);
        if (subjectInfo != null) {
            return ImmutableMap.of(
                    "error", "主题已存在",
                    "data", subjectInfo
            );
        }

        try {
            store.insertSubject(subject, tag);
            final SubjectInfo info = new SubjectInfo();
            info.setName(subject);
            info.setTag(tag);
            info.setUpdateTime(System.currentTimeMillis());
            return ImmutableMap.of("data", info);
        } catch (DuplicateKeyException e) {
            return ImmutableMap.of(
                    "error", "主题已存在",
                    "data", store.getSubjectInfo(subject)
            );
        }
    }
}
