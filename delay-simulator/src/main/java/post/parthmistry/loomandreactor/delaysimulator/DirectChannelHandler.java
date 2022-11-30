package post.parthmistry.loomandreactor.delaysimulator;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class DirectChannelHandler extends ChannelInboundHandlerAdapter {

    private ChannelHandlerContext parentCtx;

    private ChannelHandlerContext ctx;

    public DirectChannelHandler(ChannelHandlerContext ctx) {
        this.parentCtx = ctx;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
    }

    public void writeAndFlush(ByteBuf buf) {
        ctx.writeAndFlush(buf);
    }

    public void close() {
        ctx.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        parentCtx.writeAndFlush(msg);
    }

}
