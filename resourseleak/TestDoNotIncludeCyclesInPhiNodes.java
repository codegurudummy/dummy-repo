package resourceleak;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.SocketException;

public class TestDoNotIncludeCyclesInPhiNodes {

    public void testDoNotIncludeCyclesInPhiNodes() throws IOException {
        if (level != 0) // otherwise disconnected or connect
            throw new SocketException("Socket closed or already open ("+ level +")");
        IOException exception = null;
        SSLSocketFactory sf = null;
        SSLSocket s = null;
        for (int i = 0; i < ports.length && s == null; i++) {
            try {
                if (sf == null)
                    sf = getSocketFactory();
                s = (SSLSocket)sf.createSocket(host, ports[i]);
                s.startHandshake();
                exception = null;
            } catch (IOException exc) {
                if (s != null)
                    s.close();
                s = null;
                exception = exc;
            }
        }
        if (exception != null)
            throw exception; // connection wasn't successful at any port

        prepare(s);
    }
}
