package qunar.tc.qmq.netty;

import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhenwei.liu
 * @since 2019-09-23
 */
@ChannelHandler.Sharable
public class OutboundExceptionHandler extends ChannelOutboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutboundExceptionHandler.class);

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (promise != null) {
            try {
                promise.addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            } catch (Throwable t) {
                // ignore for void promise
            }
        }
        super.write(ctx, msg, promise);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOGGER.error("exception caught", cause);
        super.exceptionCaught(ctx, cause);
    }
}
