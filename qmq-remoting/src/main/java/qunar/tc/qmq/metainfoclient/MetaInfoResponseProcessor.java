package qunar.tc.qmq.metainfoclient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.codec.Serializer;
import qunar.tc.qmq.codec.Serializers;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.MetaInfoResponse;

import java.util.Collection;

/**
 * @author zhenwei.liu
 * @since 2019-09-24
 */
public class MetaInfoResponseProcessor implements MetaServerCommandDecoder.DatagramProcessor {

    private static final Logger logger = LoggerFactory.getLogger(MetaInfoResponseProcessor.class);
    private final Collection<MetaInfoClient.ResponseSubscriber> responseSubscribers;

    public MetaInfoResponseProcessor(Collection<MetaInfoClient.ResponseSubscriber> responseSubscribers) {
        this.responseSubscribers = responseSubscribers;
    }

    @Override
    public void onSuccess(Datagram datagram) {

        short version = datagram.getHeader().getVersion();
        Serializer<MetaInfoResponse> serializer = Serializers.getSerializer(MetaInfoResponse.class);
        MetaInfoResponse response = serializer.deserialize(datagram.getBody(), null, version);

        if (response != null) {
            notifySubscriber(response);
        } else {
            logger.warn("request meta info UNKNOWN. code={}", datagram.getHeader().getCode());
        }
    }

    private void notifySubscriber(MetaInfoResponse response) {
        for (MetaInfoClient.ResponseSubscriber subscriber : responseSubscribers) {
            try {
                subscriber.onSuccess(response);
            } catch (Exception e) {
                logger.error("notify meta response error", e);
            }
        }
    }
}
