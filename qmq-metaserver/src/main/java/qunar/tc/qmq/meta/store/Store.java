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

package qunar.tc.qmq.meta.store;

import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.BrokerGroupKind;
import qunar.tc.qmq.meta.BrokerState;
import qunar.tc.qmq.meta.model.SubjectInfo;
import qunar.tc.qmq.meta.model.SubjectRoute;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

import java.util.List;

/**
 * @author yunfeng.yang
 * @since 2017/8/30
 */
public interface Store {

    int insertSubjectRoute(String subject, int version, List<String> groupNames);

    int updateSubjectRoute(String subject, int version, List<String> groupNames);

    SubjectRoute selectSubjectRoute(String subject);

    void insertOrUpdateBrokerGroup(String groupName, BrokerGroupKind kind, String masterAddress, BrokerState brokerState);

    void updateBrokerGroup(String groupName, BrokerState brokerState);

    void updateBrokerGroupTag(String groupName, String tag);

    void insertSubject(String subject, String tag);

    List<BrokerGroup> getAllBrokerGroups();

    List<SubjectRoute> getAllSubjectRoutes();

    List<SubjectInfo> getAllSubjectInfo();

    SubjectInfo getSubjectInfo(final String subject);

    BrokerGroup getBrokerGroup(String groupName);

    void insertClientMetaInfo(MetaInfoRequest metaInfoRequest);
}
