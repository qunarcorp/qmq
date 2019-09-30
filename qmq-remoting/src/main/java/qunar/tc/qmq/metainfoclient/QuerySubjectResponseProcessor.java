package qunar.tc.qmq.metainfoclient;

import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.codec.Serializer;
import qunar.tc.qmq.codec.Serializers;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.QuerySubjectRequest;
import qunar.tc.qmq.protocol.QuerySubjectResponse;

import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-09-24
 */
public class QuerySubjectResponseProcessor implements MetaServerCommandDecoder.DatagramProcessor {

    private Map<String, MetaInfoClient.QuerySubjectCallback> querySubjectCallbacks = Maps.newConcurrentMap();

    public void registerCallback(QuerySubjectRequest request, MetaInfoClient.QuerySubjectCallback callback) {
        querySubjectCallbacks.put(request.getPartitionName(), callback);
    }

    @Override
    public void onSuccess(Datagram datagram) {
        ByteBuf buf = datagram.getBody();
        Serializer<QuerySubjectResponse> serializer = Serializers.getSerializer(QuerySubjectResponse.class);
        QuerySubjectResponse response = serializer.deserialize(buf, null);
        if (response != null) {
            String key = response.getPartitionName();
            MetaInfoClient.QuerySubjectCallback callback = querySubjectCallbacks.get(key);
            callback.onSuccess(response);
        }
    }
}
