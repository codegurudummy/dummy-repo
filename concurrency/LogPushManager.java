package concurrency;

import com.amazon.cp.assumptiontool.threadlocal.LogPushThreadLocal;
import com.amazon.cp.assumptiontool.tool.CloseableTool;
import com.amazon.cp.assumptiontool.tool.LogTool;
import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Singleton, but couldn't be a bean because the user LogPushAppender is not managed by Spring. <ul>
 * <li>registerClient and closeClient must be synchronized by the LogPushManager lock because there're multiple steps in each and they should be atomic</li>
 * <li>getWriter and isRegistered doesn't need to be synchronized because there's only one API call on the ConcurrentHashMap</li>
 * <li>for all the actions on PrintWriter must be protected by the object lock to make sure no multiple threads accessing it at the same time, but it's still possible to write data to or close a closed writer, both make an Exception be thrown out but it's caught and ignored: (1) connection lost; (2) rare concurrency scenario (e.g. thread A closes it after thread B getting the print writer, then B tries to write data)</li></ul>
 */
public class LogPushManager {
    private static final LogPushManager instance = new LogPushManager();
    private final Logger logger = Logger.getLogger(this.getClass());

    /**
     * key: ip, value: the writer
     * The map is for the appender to write logs.
     */
    private Map<String, PrintWriter> logPushMap = new ConcurrentHashMap<>();

    private LogPushManager() {
    }

    public static LogPushManager getInstance() {
        return instance;
    }

    /**
     * Register a new client to the map for log appender.
     * Register and close methods are "synchronized" because the behaviors on map should be atomic.
     */
    public synchronized void registerClient(String ip, PrintWriter writer) {
        logger.info(LogTool.format("Register client: {} ", ip));
        PrintWriter oldWriter = logPushMap.get(ip);
        if (oldWriter != null) {
            synchronized (oldWriter) { // the sync here is only for the wait-and-notify mechanism on the writer, unrelated to the map
                logger.info("Closing old writer...");
                CloseableTool.closeQuietly(oldWriter);
                oldWriter.notifyAll(); // closed passively, notify the controller
            }
        }

        logPushMap.put(ip, writer);
    }

    public synchronized void closeClient(String ip, PrintWriter writer) {
        logger.info(LogTool.format("Close client for {}", ip));
        if (logPushMap.get(ip) == writer) {
            CloseableTool.closeQuietly(writer);

            logPushMap.remove(ip);
        }
    }

    public PrintWriter getWriter() {
        String ip = LogPushThreadLocal.get();
        if (ip == null)
            return null;

        return logPushMap.get(ip);
    }

    public boolean isRegistered() {
        return this.getWriter() != null;
    }
}