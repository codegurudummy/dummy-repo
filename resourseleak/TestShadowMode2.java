package resourceleak;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DateFormat;
import java.util.Date;
import java.util.Map;

public class TestShadowMode2 {



    public void testShadowMode10(Map<String, String> entry) {
        if (entry.isEmpty()) {
            return;
        }

        TSDMetrics tsdMetric = new TSDMetrics(t3MetricsFactory);
        // set up marketplace
        tsdMetric.addProperty("Marketplace", location);
        int blackwatchBGPEstablishedCounter = 0;
        for (String counterName : entry.keySet()) {
            try {
                long counterValue = Long.valueOf(entry.get(counterName));
                // sum all BGP state counter together
                if (counterName.contains(BGP_STATE_COUNTER_NAME)) {
                    blackwatchBGPEstablishedCounter += counterValue;
                }
                tsdMetric.addCount(counterName, counterValue);
            } catch (NumberFormatException ex) {
                LOG.warn("unexpected counter value : " + entry.get(counterName));
            }
        }

        // add counter for All BGP state counter
        tsdMetric.addCount(BGP_STATE_COUNTER_NAME, blackwatchBGPEstablishedCounter);

        // add counter for number of active host metric
        // As each host maps 2 * portCount (number of ports in a host) established BGP session,
        // so number of active host = (number of established BGP session) / (2 * portCountPerHost)
        if (portsCount != 0) {
            tsdMetric.addCount(
                    BlackWatchCommon.ACTIVE_HOST_METRIC_NAME,
                    (float) blackwatchBGPEstablishedCounter / (2 * portsCount));
        }

        tsdMetric.end();
    }


    public static void testShadowMode11(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("USAGE: TraceFormatter trace_file");
            System.exit(2);
        }
        FileChannel fc = new FileInputStream(args[0]).getChannel();
        while (true) {
            ByteBuffer bb = ByteBuffer.allocate(41);
            fc.read(bb);
            bb.flip();

            byte app = bb.get();
            long time = bb.getLong();
            long id = bb.getLong();
            int cxid = bb.getInt();
            long zxid = bb.getLong();
            int txnType = bb.getInt();
            int type = bb.getInt();
            int len = bb.getInt();
            bb = ByteBuffer.allocate(len);
            fc.read(bb);
            bb.flip();
            String path = "n/a";
            if (bb.remaining() > 0) {
                if (type != OpCode.createSession) {
                    int pathLen = bb.getInt();
                    byte b[] = new byte[pathLen];
                    bb.get(b);
                    path = new String(b);
                }
            }
            System.out.println(DateFormat.getDateTimeInstance(DateFormat.SHORT,
                    DateFormat.LONG).format(new Date(time))
                    + ": "
                    + (char) app
                    + " id=0x"
                    + Long.toHexString(id)
                    + " cxid="
                    + cxid
                    + " op="
                    + op2String(type)
                    + " zxid=0x"
                    + Long.toHexString(zxid)
                    + " txnType="
                    + txnType
                    + " len="
                    + len + " path=" + path);
        }
    }


}
