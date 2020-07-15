package concurrency;

import java.io.IOException;

public class TestMUDetect01 {
//    class ReaderPool implements Closeable {
//        public int foo(int x) {
//            return x + (int)System.currentTimeMillis();
//        }
//        private boolean car() {
//            return foo(33) > 33;
//        }
//        public int bar(int x) {
//            int res = foo(x);
//            foo(x);
//            assert car();
//            return res;
//        }        
//    }
    
//    public void onClose(ShardId shardId) {
//        assert empty(shardStats.get(shardId));
//        shardStats.remove(shardId);
//    }

//    private EmailServiceProxy removeProxyIfDead(String protocol) {
//        synchronized(sConnectivityLock) {
//            EmailServiceProxy proxy = mProxyCache.get(protocol);
//            return (proxy != null && proxy.isDead()) ? mProxyCache.remove(protocol) : null;
//        }
//    }

    public void register(String name, String jsonSpecification)
    {
        if (templates.containsKey(name))
        {
            throw new IllegalStateException("Attempt to register a template twice for name '"+name+"'");
        }

        try
        {
            LandingTemplate template = LandingTemplate.fromJson(jsonSpecification);
            templates.put(name, template);
        }
        catch (final IOException | IllegalArgumentException e)
        {
            throw new IllegalArgumentException("Attempt to register template with name " + name + " failed.", e);
        }
    }

}
