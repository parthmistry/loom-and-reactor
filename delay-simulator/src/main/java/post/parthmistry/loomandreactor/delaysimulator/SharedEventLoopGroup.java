package post.parthmistry.loomandreactor.delaysimulator;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

public class SharedEventLoopGroup {

    public static final EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);

}
