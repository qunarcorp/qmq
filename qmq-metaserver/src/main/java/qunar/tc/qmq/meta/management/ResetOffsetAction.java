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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.PartitionProps;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.ProducerAllocation;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.model.SubjectRoute;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.netty.NettyClientConfig;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingHeader;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import javax.servlet.http.HttpServletRequest;
import java.util.Collection;

public class ResetOffsetAction implements MetaManagementAction {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResetOffsetAction.class);

    private final Store store;
    private final CachedMetaInfoManager cachedMetaInfoManager;
    private final NettyClient client;

    public ResetOffsetAction(Store store, CachedMetaInfoManager cachedMetaInfoManager) {
        this.cachedMetaInfoManager = cachedMetaInfoManager;
        this.client = NettyClient.getClient();
        this.client.start(new NettyClientConfig());
        this.store = store;
    }

    @Override
    public ActionResult<String> handleAction(HttpServletRequest req) {
        String subject = req.getParameter("subject");
        String consumerGroup = req.getParameter("group");
        int action = Integer.valueOf(req.getParameter("code"));

        if (Strings.isNullOrEmpty(subject) || Strings.isNullOrEmpty(consumerGroup)) {
            return ActionResult.error("subject and consumerGroup required");
        }

        if (action != 1 && action != 2) {
            return ActionResult.error("action must 1 or 2, LATEST=1, EARLIEST=2");
        }

        ProducerAllocation producerAllocation = cachedMetaInfoManager.getProducerAllocation(ClientType.PRODUCER, subject);
        Collection<PartitionProps> partitionProps = producerAllocation.getLogical2SubjectLocation().asMapOfRanges().values();
        for (PartitionProps partitionProp : partitionProps) {
            String partitionName = partitionProp.getPartitionName();
            final SubjectRoute subjectRoute = store.selectSubjectRoute(partitionName);
            if (subjectRoute == null) {
                return ActionResult.error("find no route");
            }

            Datagram datagram = buildResetOffsetDatagram(subject, consumerGroup, action);
            String brokerGroupName = partitionProp.getBrokerGroup();
            try {
                final BrokerGroup brokerGroup = store.getBrokerGroup(brokerGroupName);
                client.sendSync(brokerGroup.getMaster(), datagram, 2000);
                LOGGER.error("reset offset successful subject {}, partitionName {}, brokerGroupName {}", subject, partitionName, brokerGroupName);
            } catch (Throwable e) {
                LOGGER.error("send consume manage request error, subject {} partitionName {} brokerGroupName {}", subject, partitionName, brokerGroupName, e);
                return ActionResult.error("reset failed: brokerGroupName=" + brokerGroupName);
            }

        }
        return ActionResult.ok("success");
    }

    private Datagram buildResetOffsetDatagram(final String subject, final String consumerGroup, int code) {
        final Datagram datagram = new Datagram();
        final RemotingHeader header = new RemotingHeader();
        header.setCode(CommandCode.CONSUME_MANAGE);
        datagram.setHeader(header);
        datagram.setPayloadHolder(out -> {
            PayloadHolderUtils.writeString(subject, out);
            PayloadHolderUtils.writeString(consumerGroup, out);
            out.writeInt(code);
        });
        return datagram;
    }
}
