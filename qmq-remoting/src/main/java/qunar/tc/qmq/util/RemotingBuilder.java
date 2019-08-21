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

package qunar.tc.qmq.util;

import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.protocol.RemotingCommandType;
import qunar.tc.qmq.protocol.RemotingHeader;

/**
 * @author yunfeng.yang
 * @since 2017/7/6
 */
public class RemotingBuilder {
    public static Datagram buildRequestDatagram(short code, PayloadHolder payloadHolder) {
        final Datagram datagram = new Datagram();
        datagram.setHeader(buildRemotingHeader(code, RemotingCommandType.REQUEST_COMMAND.getCode(), 0));
        datagram.setPayloadHolder(payloadHolder);
        return datagram;
    }

    private static RemotingHeader buildRemotingHeader(short code, int flag, int opaque) {
        final RemotingHeader header = new RemotingHeader();
        header.setCode(code);
        header.setFlag(flag);
        header.setOpaque(opaque);
        header.setRequestCode(code);
        header.setVersion(RemotingHeader.VERSION_10);
        return header;
    }

    public static Datagram buildResponseDatagram(final short code, final RemotingHeader requestHeader, final PayloadHolder payloadHolder) {
        final Datagram datagram = new Datagram();
        datagram.setHeader(buildResponseHeader(code, requestHeader));
        datagram.setPayloadHolder(payloadHolder);
        return datagram;
    }

    public static Datagram buildEmptyResponseDatagram(final short code, final RemotingHeader requestHeader) {
        return buildResponseDatagram(code, requestHeader, null);
    }

    public static RemotingHeader buildResponseHeader(final short code, final RemotingHeader requestHeader) {
        return buildRemotingHeader(code, RemotingCommandType.RESPONSE_COMMAND.getCode(), requestHeader);
    }

    private static RemotingHeader buildRemotingHeader(final short code, final int flag, final RemotingHeader requestHeader) {
        final RemotingHeader header = new RemotingHeader();
        header.setCode(code);
        header.setFlag(flag);
        header.setOpaque(requestHeader.getOpaque());
        header.setVersion(requestHeader.getVersion());
        header.setRequestCode(requestHeader.getCode());
        return header;
    }
}
