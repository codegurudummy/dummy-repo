package concurrency;

import com.amazon.brazil.gitfarm.err.GFRuntimeException;
import com.amazon.brazil.gitfarm.fleet.NetworkHealthManager.Status;
import com.amazon.brazil.gitfarm.service.GitFarmException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import s3.dfddclient.DFDDClient;
import s3.dfddclient.HealthChecker;
import s3.dfddclient.InstanceRecord;
import s3.dfddclient.InstanceRecord.State;
import s3.dfddclient.ValidateClique;
import s3.stumpy.StumpyEndpoint;
import s3.stumpy.StumpyManager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Component("GitFarmDFDDClient")
public class GitFarmDFDDClientBean implements GitFarmDFDDClient, DisposableBean  {

    public static final int BUFFER_TIME_S = 15;

    public static final String DFDD_APP_NAME = "GitFarmService";

    private static final Log LOG = LogFactory.getLog(GitFarmDFDDClientBean.class);

    private DFDDClient dfdd;
    private DFDDInstance thisInstance;
    private HealthChecker healthChecker;
    private final ConcurrentHashMap<String, DFDDInstance> peerToInstance = new ConcurrentHashMap<String, DFDDInstance>();
    private final Set<InstanceStatusChangeListener> listeners =
            new HashSet<InstanceStatusChangeListener>();

    private final Set<String> knownPeers = new HashSet<String>();

    private final ScheduledExecutorService bufferingSheduler;
    private final HashMap<String, HeartbeatInstance> bufferedMessages = new HashMap<String, GitFarmDFDDClientBean.HeartbeatInstance>();

    public GitFarmDFDDClientBean() {
        bufferingSheduler = Executors.newScheduledThreadPool(2);
    }

    /**
     * Constructor for testing only.
     */
    protected GitFarmDFDDClientBean(ScheduledExecutorService exec) {
        bufferingSheduler = exec;
    }

    /** Called by Spring. */
    public void destroy() {
        LOG.info("Shutting down");
        bufferingSheduler.shutdownNow();
    }

    public void setDFDDInstance(DFDDInstance thisInstance) {
        if (isRunning()) {
            throw new IllegalStateException("Cannot set thisInstance while running");
        }
        if (thisInstance == null) {
            throw new IllegalArgumentException("Null DFDDInstance");
        }
        if (thisInstance.getStartTime() == null) {
            throw new IllegalStateException("DFDDInstance must specify a start time");
        }

        this.thisInstance = thisInstance;

        this.thisInstance.setStatus(Status.TIS_ME);
    }

    // This should be the DFDDNetworkHealthManager.
    // The SshHealthCheck is also a HealthChecker, but isn't a Spring bean, so
    // it's not a candidate for autowiring.
    @Autowired
    public void setHealthChecker(HealthChecker healthChecker) {
        if (isRunning()) {
            throw new IllegalStateException("Cannot set health checker while running");
        }
        if (healthChecker == null) {
            throw new IllegalArgumentException("Null HealthChecker");
        }
        this.healthChecker = healthChecker;
    }

    @Override
    public void addInstanceStatusChangeListener(InstanceStatusChangeListener newListener) {
        listeners.add(newListener);
    }
    @Override
    public void removeInstanceStatusChangeListener(InstanceStatusChangeListener oldListener) {
        listeners.remove(oldListener);
    }

    @Override
    public void start() {
        if (!isRunning()) {
            if (healthChecker == null) {
                throw new IllegalStateException("Must specify HealthChecker first");
            }

            dfdd = createDFDDClient();

            verifyDFDDIsReady();

            LOG.info(String.format("%s: Started up", this));
        }
    }

    // Throw a GFRuntimeException if we are unable to ping DFDD. This method exists primarily
    // to allow for overriding in a test class.
    protected void verifyDFDDIsReady() {
        try {
            StumpyEndpoint ping = new StumpyEndpoint("localhost", 2977);
            ping.ping(null);
            ping.waitForPong(MILLISECONDS.convert(BUFFER_TIME_S, SECONDS));
            ping.closeChannel();
        } catch (Exception e) {
            throw new GFRuntimeException("Interrupted while waiting for DFDD messages. Failing startup", e);
        }
    }

    @Override
    public void stop() {
        if (isRunning()) {
            LOG.info(String.format("%s:Shutting down", this));
            // So far I haven't found a corresponding 'unsubscribe'
            dfdd.removeBeatingApplication(DFDD_APP_NAME, this.thisInstance.toInstanceID());

            try {
                dfdd.close();
            } catch (Exception ignored) {}

            dfdd = null;

            bufferingSheduler.shutdown();
        }
    }

    /** Tests may override this */
    protected DFDDClient createDFDDClient() {
        ValidateClique.validateServerClique.set(false);

        try {
            StumpyManager stumpy = StumpyManager.newManyThreadManager(4);
            DFDDClient newClient = DFDDClient.createManagedDFDDClient(stumpy);

            newClient.addBeatingApplication(DFDD_APP_NAME, this.thisInstance.toInstanceID(), this.thisInstance.getHostname(), healthChecker);
            LOG.info(String.format("%s: Started heartbeat", this));

            newClient.subscribe(DFDD_APP_NAME, this);

            return newClient;
        } catch (IOException e) {
            throw new GitFarmException(e);
        }
    }

    @Override
    public boolean isRunning() {
        return dfdd != null;
    }

    @Override
    public DFDDInstance getInstanceByPeer(String hostname) {
        return peerToInstance.get(hostname);
    }

    @Override
    public int size() {
        return peerToInstance.size();
    }

    @Override
    public Collection<DFDDInstance> getInstances() {
        return Collections.unmodifiableCollection(peerToInstance.values());
    }

    @Override
    public void inform(InstanceRecord record) {
        HeartbeatInstance heartBeatInst = new HeartbeatInstance(record);
        String heartBeatInstPeer = heartBeatInst.getHostname();

        synchronized (bufferedMessages) {
            HeartbeatInstance extant = bufferedMessages.get(heartBeatInstPeer);

            if (extant == null) {
                bufferedMessages.put(heartBeatInstPeer, heartBeatInst);
                bufferingSheduler.schedule(new BufferingRunnable(heartBeatInst), BUFFER_TIME_S, SECONDS);
            } else if (heartBeatInst.isNewerThan(extant)) {
                bufferedMessages.put(heartBeatInstPeer, heartBeatInst);
            } // else we have a less correct message
        }
    }

    private class BufferingRunnable implements Runnable{
        private final String hostname;
        public BufferingRunnable(HeartbeatInstance instance) {
            this.hostname = instance.getHostname();
        }

        @Override
        public void run() {
            HeartbeatInstance instance = null;
            synchronized (bufferedMessages) {
                instance = bufferedMessages.remove(hostname);
            }

            if (instance == null) {
                LOG.warn("Buffered DFDD message missing");
            } else {
                doInform(instance);
            }
        }
    }

    private void doInform(HeartbeatInstance heartBeatInstance) {
        boolean me = this.thisInstance.toInstanceID().equals(heartBeatInstance.toInstanceID());

        try {
            if (heartBeatInstance.getHostname().equals(this.thisInstance.getHostname()) && !me) {
                //We have a message about this host, which could be bad news.
                if (thisInstance.isNewerThan(heartBeatInstance)) {
                    // ignore message completely.
                    return;
                } else {
                    LOG.error(String.format("Received message about an instance on this host which is more recent than this instance. DIE! thisInstance = [%s], heartBeatInstance = [%s]", thisInstance, heartBeatInstance));
                    fireDieDieDie();
                }
            }

            switch (heartBeatInstance.getState()) {
                case OK:
                    addOrUpdateInstanceIfNewer(heartBeatInstance, Status.HEALTHY);
                    break;
                case InCommunicado:
                    addOrUpdateInstanceIfNewer(heartBeatInstance, Status.DEAD);
                    break;
                case Fail:
                    handleDead(heartBeatInstance, me);
                    break;
                case Forgotten:
                    handleForgotten(heartBeatInstance, me);
                    break;
                default:
                    // If we ever get here, then DFDD added a new state and we
                    // haven't updated the code to match.  This is almost
                    // guaranteed to be a bug.
                    LOG.error(String.format("Unknown DFDD record status: %s", heartBeatInstance.getState()));
            }
        } catch (IllegalArgumentException e) {
            LOG.error(String.format("%s got bogus instanceID", this), e);
        }
    }

    @Override
    public void override(String hostInfo, State state) {
        DFDDInstance instance = getInstanceByPeer(hostInfo);
        if (instance != null) {
            dfdd.override(DFDD_APP_NAME, instance.toInstanceID(), state);
        }
    }

    @Override
    public void kill(DFDDInstance inst) {
        DFDDInstance currentInst = peerToInstance.get(inst.getHostname());

        if (inst.equals(currentInst)) {
            dfdd.kill(DFDD_APP_NAME, inst.toInstanceID());
        }
    }

    protected void handleDead(final DFDDInstance other, boolean me) {
        if (me) {
            fireDieDieDie();
        } else {
            addOrUpdateInstanceIfNewer(other, Status.LONGDEAD);
        }
    }

    protected void handleForgotten(final DFDDInstance other, boolean me) {
        if (me) {
            // Should never get to this point...
            fireDieDieDie();
        } else {
            removeInstanceIfNewer(other);
        }
    }

    private void addOrUpdateInstanceIfNewer(DFDDInstance newEntry, Status newStatus) {
        newEntry.setStatus(newStatus);
        final DFDDInstance oldEntry = peerToInstance.get(newEntry.getHostname());

        if (oldEntry == null) {
            if (peerToInstance.putIfAbsent(newEntry.getHostname(), newEntry) == null) {
                LOG.info(String.format("%s adding/updating peer %s", this, newEntry));
                addKnownPeer(newEntry);
                fireInstanceStatusChange(newEntry);
            } else {
                addOrUpdateInstanceIfNewer(newEntry, newStatus);
            }
        } else if (oldEntry.isNewerThan(newEntry)) {
            LOG.debug(String.format("%s ignoring %s %s in favor of %s", this, newStatus, newEntry, oldEntry));
        } else if ( peerToInstance.replace(newEntry.getHostname(), oldEntry, newEntry)) {
            if (oldEntry.toInstanceID().equals(newEntry.toInstanceID()) &&
                    oldEntry.getStatus() == newEntry.getStatus()) {
                LOG.debug(String.format("%s replaced %s with newer, %s (status unchanged)", this, oldEntry, newEntry));
            } else {
                LOG.info(String.format("%s replaced %s with newer, %s %s", this, oldEntry, newStatus, newEntry));
                fireInstanceStatusChange(newEntry);
            }
        } else {
            addOrUpdateInstanceIfNewer(newEntry, newStatus);
        }
    }

    private void removeInstanceIfNewer(DFDDInstance toRemove) {
        String toRemovePeer = toRemove.getHostname();
        final DFDDInstance oldEntry = peerToInstance.get(toRemovePeer);

        if (oldEntry == null) {
            LOG.debug(String.format("%s ignoring news that unknown endpoint %s is forgotten", this, toRemovePeer));
            return;
        }

        if (oldEntry.isNewerThan(toRemove)) {
            LOG.debug(String.format("%s ignoring forgotten %s in favor of previous, newer %s", this, toRemove, oldEntry));
        } else {
            LOG.info(String.format("%s removing forgotten instance %s", this, toRemove));

            if ( peerToInstance.remove(toRemovePeer, oldEntry)) {
                fireInstanceIsForgotten(toRemove);
            } else {
                removeInstanceIfNewer(toRemove);
            }
        }
    }

    private void fireInstanceStatusChange(DFDDInstance instance) {
        for (InstanceStatusChangeListener listener : listeners) {
            switch (instance.getStatus()) {
                case HEALTHY:
                    listener.instanceIsOK(instance);
                    break;
                case DEAD:
                    listener.instanceIsDead(instance);
                    break;
                case LONGDEAD:
                    listener.instanceIsLongDead(instance);
                    break;
                case TIS_ME:
                default:
                    // This would be a bug
                    LOG.error(String.format("Can't notify listeners of status %s", instance.getStatus()));
            }
        }
    }

    private void fireInstanceIsForgotten(DFDDInstance instance) {
        instance.setStatus(Status.LONGDEAD);
        for (InstanceStatusChangeListener listener : listeners) {
            listener.instanceIsForgotten(instance);
        }
    }

    private void fireDieDieDie() {
        for (InstanceStatusChangeListener listener : listeners) {
            listener.diediedie();
        }
    }

    @Override
    public String toString() {
        return String.format("GitFarmDFDDClientBean %s with %s known instances", this.thisInstance == null ? "<not running>" : this.thisInstance, peerToInstance.size());
    }

    @Override
    public boolean isPeerAddress(String address) {
        return knownPeers.contains(address);
    }

    private void addKnownPeer(DFDDInstance instance) {
        String hostname = instance.getHostname();

        if (!knownPeers.contains(hostname)) {
            knownPeers.add(hostname);

            try {
                String ip = InetAddress.getByName(hostname).getHostAddress();
                knownPeers.add(ip);
            } catch (UnknownHostException e) {
                LOG.warn(String.format("IP resolution failed for: %s", hostname), e);
            }
        }
    }

    class HeartbeatInstance extends DFDDInstance {
        public final long heartbeatCount;
        public final State state;

        public HeartbeatInstance(InstanceRecord record) {
            super(record.instanceId);

            this.state = record.status;
            this.heartbeatCount = record.heartbeatCount;
        }

        @Override
        protected String getHeartBeatStr() {
            return String.format("|%s", this.heartbeatCount);
        }

        protected InstanceRecord.State getState() {
            return state;
        }

        protected long getHeartbeat() {
            return heartbeatCount;
        }

        @Override
        public boolean isNewerThan(DFDDInstance other) {
            int result = super.compareStartTimes(other);
            if (result != 0) {
                return result > 0;
            }

            if (other instanceof HeartbeatInstance) {
                return Comparator.comparing(HeartbeatInstance::getHeartbeat)
                  .compare(this, (HeartbeatInstance) other) > 0;
            }

            return false;
        }
    }
}
