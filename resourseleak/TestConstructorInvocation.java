package resourceleak;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;


public class TestConstructorInvocation {

    public testConstructorInvocation1(String host, int port, int timeout) throws IOException {
        this(SocketChannel.open(), timeout, new InetSocketAddress(host, port));
    }

    public testConstructorInvocation2(IOExecutor executor) throws IOException {
        this(executor, Selector.open());
    }

}
