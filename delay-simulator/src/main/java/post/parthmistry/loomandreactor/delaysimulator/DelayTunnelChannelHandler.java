package post.parthmistry.loomandreactor.delaysimulator;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DelayTunnelChannelHandler extends ChannelInboundHandlerAdapter {

    private static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private DirectChannelHandler directChannelHandler;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("channelActive");
        Bootstrap b = new Bootstrap();
        b.group(SharedEventLoopGroup.eventLoopGroup);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                directChannelHandler = new DirectChannelHandler(ctx);
                ch.pipeline().addLast(directChannelHandler);
            }
        });
        b.connect("localhost", 5432);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("channelInactive");
        directChannelHandler.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buf = (ByteBuf) msg;
        executor.schedule(() -> {
            directChannelHandler.writeAndFlush(buf);
        }, 250, TimeUnit.MILLISECONDS);
    }

}
