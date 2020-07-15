package concurrency;

import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.preference.PreferenceManager;
import android.support.annotation.MainThread;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.support.annotation.WorkerThread;
import com.amazon.clouddrive.extended.AmazonCloudDriveExtended;
import com.amazon.gallery.foundation.metrics.ComponentProfiler;
import com.amazon.gallery.foundation.metrics.Profiler;
import com.amazon.gallery.foundation.utils.apilevel.BuildFlavors;
import com.amazon.gallery.foundation.utils.log.GLogger;
import com.amazon.gallery.framework.PhotosApplication;
import com.amazon.gallery.framework.data.dao.mediaitem.MediaItemDao;
import com.amazon.gallery.framework.data.family.FamilyMetricsHelper;
import com.amazon.gallery.framework.gallery.metrics.ProfilerSession;
import com.amazon.gallery.framework.kindle.Keys;
import com.amazon.gallery.framework.kindle.KindleFrameworkConstants;
import com.amazon.gallery.framework.kindle.auth.AuthenticationManager;
import com.amazon.gallery.framework.kindle.notifications.UnifiedUploadNotificationContainer;
import com.amazon.gallery.framework.metrics.MetricTag;
import com.amazon.gallery.framework.metrics.customer.CustomerMetricsHelper;
import com.amazon.gallery.framework.model.media.MediaItem;
import com.amazon.gallery.framework.model.media.MediaItemUtil;
import com.amazon.gallery.framework.network.upload.UploadPreferences;
import com.amazon.gallery.thor.app.ThorGalleryApplication;
import com.amazon.gallery.thor.cds.CloudDriveServiceClientManager;
import com.amazon.gallery.thor.cds.ThorMixtapeMetricRecorder;
import com.amazon.gallery.thor.deduplication.DigestTypeResolver;
import com.amazon.gallery.thor.metrics.TuneMetrics;
import com.amazon.gallery.thor.upload.UploadServiceVersionResolver;
import com.amazon.mixtape.metrics.MixtapeMetricRecorder;
import com.amazon.mixtape.upload.*;
import com.amazon.photos.R;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Gallery implementation of the Mixtape.UploadService used to manage the application's upload queues.
 */
public class GalleryUploadService extends UploadService {

    private static final String TAG = GalleryUploadService.class.getName();

    private AuthenticationManager authenticationManager;
    private CloudDriveServiceClientManager cloudDriveServiceClientManager;

    private ThorMixtapeMetricRecorder metricsRecorder;
    private GalleryUploadStatusTracker uploadStatusTracker;
    private GalleryUploadProgressListener uploadProgressListener;
    private UploadConflictResolver uploadConflictResolver;
    private UnifiedUploadNotificationContainer uploadNotificationContainer;
    private DigestTypeResolver digestTypeResolver;
    private CaptiveNetworkDetector captiveNetworkDetector;
    private UploadServiceVersionResolver uploadServiceVersionResolver;
    private final GalleryUploadServiceConfiguration configuration;

    private ProfilerSession session;
    private MediaItemDao mediaItemDao;
    private ComponentProfiler componentProfiler;

    /** The set of UploadQueues */
    private final ConcurrentMap<String, Set<UploadQueue>> mQueueMap = new ConcurrentHashMap<>();

    private final UploadServiceStateObserver stateObserver = new StateObserver();

    public GalleryUploadService() {
        configuration = new GalleryUploadServiceConfigurationWrapper();
    }

    @Override
    public void onCreate() {
        GLogger.i(TAG, "Created...");
        super.onCreate();
        session = ThorGalleryApplication.getBean(Keys.PROFILER_SESSION);
        authenticationManager = ThorGalleryApplication.getBean(Keys.AUTHENTICATING_MANAGER);
        cloudDriveServiceClientManager = ThorGalleryApplication.getBean(Keys.CLOUD_DRIVE_SERVICE_CLIENT_MANAGER);
        uploadStatusTracker = ThorGalleryApplication.getBean(Keys.GALLERY_UPLOAD_STATUS_TRACKER);
        Profiler profiler = ThorGalleryApplication.getBean(Keys.PROFILER);
        metricsRecorder = new ThorMixtapeMetricRecorder(profiler);
        captiveNetworkDetector = PhotosApplication.getAppComponent().getCaptiveNetworkDetector();
        componentProfiler = new ComponentProfiler(profiler, GalleryUploadService.class);
        TuneMetrics tuneMetrics = PhotosApplication.getAppComponent().getTuneMetrics();
        uploadProgressListener = new GalleryUploadProgressListener(
            this.componentProfiler, uploadStatusTracker, captiveNetworkDetector,
            PreferenceManager.getDefaultSharedPreferences(this), tuneMetrics,
            PhotosApplication.getAppComponent().getActiveActivityTracker());
        uploadNotificationContainer = OttoAwareUnifiedUploadNotificationContainer.create(
                getApplicationContext(), profiler);
        uploadConflictResolver = PhotosApplication.getAppComponent().getUploadConflictResolver();
        mediaItemDao = PhotosApplication.getAppComponent().getMediaItemDao();
        digestTypeResolver = PhotosApplication.getAppComponent().getDigestTypeResolver();
        uploadServiceVersionResolver = PhotosApplication.getAppComponent().getUploadServiceVersionResolver();

        addStateObserver(stateObserver);
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        GLogger.i(TAG, "Started...");
        uploadNotificationContainer.onUploadServiceStarted(GalleryUploadService.this);

        return super.onStartCommand(intent, flags, startId);
    }

    @Override
    public void onDestroy() {
        GLogger.i(TAG, "Destroyed...");
        removeStateObserver(stateObserver);
        super.onDestroy();
        if (uploadNotificationContainer != null) {
            uploadNotificationContainer.onUploadServiceNotRunning();
        }
    }

    @Override
    public void onTaskRemoved(Intent rootIntent) {
        super.onTaskRemoved(rootIntent);
        GLogger.d(TAG, "onTaskRemoved");

        if (uploadNotificationContainer != null) {
            uploadNotificationContainer.clearNotification();
        }
    }

    @Override
    protected String getUploadProviderAuthority() {
        return getString(R.string.mixtape_sync_authority);
    }

    @Override
    protected String getCurrentAccountId() {
        return authenticationManager.getAccountId();
    }

    @Override
    protected AmazonCloudDriveExtended getAmazonCloudDriveExtendedClient(final String accountId) {
        return cloudDriveServiceClientManager.getBackgroundCdsClient(accountId);
    }

    @Override
    protected Set<UploadQueue> getUploadQueues(final String accountId) {
        Set<UploadQueue> queues = mQueueMap.get(accountId);

        if (queues == null) {
            queues = new HashSet<>();
            queues.add(createUploadQueue(QueueType.MANUAL, accountId));
            queues.add(createUploadQueue(QueueType.MANUAL_WIFI_ONLY, accountId));
            queues.add(createUploadQueue(QueueType.FORCE_UPLOAD, accountId));
            queues.add(createUploadQueue(QueueType.AUTO_SAVE_PHOTOS, accountId));
            queues.add(createUploadQueue(QueueType.AUTO_SAVE_VIDEOS, accountId));
            mQueueMap.putIfAbsent(accountId, queues);
        }

        return queues;
    }

    @Override
    protected int getMaxUploadThreadCount() {

        int debugOverride = -1;

        if(BuildFlavors.isDebug()) {
            debugOverride = getBaseContext().getSharedPreferences(KindleFrameworkConstants.SHARED_PREFERENCES, Context.MODE_PRIVATE)
                    .getInt(GalleryUploadServiceConfiguration.UPLOAD_THREAD_COUNT_OVERRIDE_KEY, -1);
        }

        int numUploadThreads = debugOverride > 0 ? debugOverride : configuration.getNumberOfUploadThreads();
        GLogger.i(TAG, "Number of upload threads used: %s", numUploadThreads);
        return numUploadThreads;
    }

    @Override
    @WorkerThread
    protected UploadServiceVersion getUploadServiceVersion() {
        return uploadServiceVersionResolver.getVersion();
    }

    @Override
    @WorkerThread
    protected DigestType getDigestType() {
        return digestTypeResolver.isVisualDigestEnabled() ? DigestType.VISUAL_DIGEST : DigestType.MD5;
    }

    private UploadQueue createUploadQueue(QueueType type, String accountId) {
        return UploadQueue.newBuilder().name(type.name()).accountId(accountId).priority(type.priority).blockers(UploadQueue.NO_BLOCKERS).build();
    }

    @Override
    protected MixtapeMetricRecorder getMixtapeMetricsRecorder() {
        return metricsRecorder;
    }

    @Override
    protected Set<String> getBlockers(final UploadQueue queue, final QueueBlockUtilities utilities) {
        final Set<String> blockers = new LinkedHashSet<>();

        // Block every queue if still in cold boot.
        if (!IGalleryUploadManager.isAllowedToUpload(getBaseContext(), getCurrentAccountId())) {
            blockers.add(MixtapeBlockers.QueueBlockers.AWAITING_READY_STATE.name());
            return blockers;
        }

        QueueType queueType = QueueType.valueOf(queue.getName());
        boolean checkForWan = false;
        switch (queueType) {
            case AUTO_SAVE_VIDEOS:
                // Auto save videos are always wifi only.
                checkForWan = true;
                // Fall through
            case AUTO_SAVE_PHOTOS:
                SharedPreferences sharedPrefs = PreferenceManager.getDefaultSharedPreferences(this);

                if (sharedPrefs.getBoolean(UploadPreferences.AUTO_UPLOAD_ONLY_WHEN_CHARGING_KEY,
                        UploadPreferences.AUTO_UPLOAD_ONLY_WHEN_CHARGING_DEFAULT_VALUE)) {
                    addBlockerIfPresent(blockers, BatteryPowerChecker.getNotChargingBlocker());
                }

                checkForWan = checkForWan || !sharedPrefs.getBoolean(UploadPreferences.WAN_ALLOWED_KEY,
                        UploadPreferences.WAN_ALLOWED_DEFAULT_VALUE);
                if (checkForWan) {
                    addBlockerIfPresent(blockers, utilities.blockWan());
                }

                addBlockerIfPresent(blockers, BatteryPowerChecker.getBatteryBlocker());

                // Some customers have power saver always enabled. This will block uploads completely.
                // We should disable this feature until we come up with a better story around it.
                //MixtapeBlockers.QueueBlockers lowPowerBlocker = batteryPowerChecker.getPowerBlocker();
                //if (lowPowerBlocker != null) {
                //    blockers.add(lowPowerBlocker.name());
                //}
                break;

            case MANUAL_WIFI_ONLY:
                addBlockerIfPresent(blockers, utilities.blockWan());
                // Fall through
            case MANUAL:
            case FORCE_UPLOAD:
                // No explicit blockers defined by us for these queues.
                // Mixtape already enforces NO-NETWORK blocker on all queues
                break;
        }

        addBlockerIfPresent(blockers, captiveNetworkDetector.blockCaptiveNetwork());

        return blockers;
    }

    @Override
    protected boolean isValidUploadRequest(UploadRequest uploadRequest) {
        if (uploadRequest != null && uploadRequest.getFileUri() != null
                && !StringUtils.isEmpty(uploadRequest.getFileUri().getPath())) {

            File file = new File(uploadRequest.getFileUri().getPath());
            if (file.exists()) {
                List<MediaItem> allItemByPath = mediaItemDao.getItemsByLocalPath(file.getPath()).getMediaItems();

                if (allItemByPath == null || allItemByPath.size() < 1) {
                    componentProfiler.trackEvent(MetricEvent.InvalidRequestMediaItemNotFound);
                } else if (allItemByPath.size() > 1) {
                    componentProfiler.trackEvent(MetricEvent.InvalidRequestMultipleItemWithSamePath);
                } else if (MediaItemUtil.isCloudMediaItem(allItemByPath.get(0))) {
                    componentProfiler.trackEvent(MetricEvent.InvalidRequestItemExists);
                } else {
                    return true;
                }
            } else {
                componentProfiler.trackEvent(MetricEvent.InvalidRequestFileNotFound);
            }
        } else {
            componentProfiler.trackEvent(MetricEvent.InvalidUploadRequestState);
        }
        return false;
    }

    /**
     * Adds a {@link MixtapeBlockers.QueueBlockers} to a set of blockers, if present.
     * @param blockers The set of blockers.
     * @param queueBlocker The blocker to add (if it is not null).
     */
    private void addBlockerIfPresent(final Set<String> blockers,
            @Nullable final MixtapeBlockers.QueueBlockers queueBlocker) {
        if (queueBlocker != null) {
            blockers.add(queueBlocker.name());
        }
    }

    private class StateObserver implements UploadServiceStateObserver {
        private State previousState = State.NOT_RUNNING;

        @Override
        public void onUploadServiceStateChanged(@NonNull final State state) {
            if (state == previousState) {
                return;
            }

            GLogger.i(TAG, "State changed to: " + state);

            if (state == State.RUNNING) {
                uploadNotificationContainer.onUploadServiceRunning(GalleryUploadService.this);

                setBackgroundListener(uploadProgressListener);
                setConflictResolver(uploadConflictResolver);

                uploadStatusTracker.onStart();

                GLogger.v(TAG, "Starting Upload Metrics");
                session.onServiceStarted();

            } else if (state == State.NOT_RUNNING) {
                uploadStatusTracker.onStop();

                // Metrics: How many items were added to the archive.
                final int addedToFA = uploadProgressListener.getTotalAddedToFamilyArchive();
                if (addedToFA > 0) {
                    metricsRecorder.getProfiler().trackEvent(FamilyMetricsHelper.COMPONENT,
                            FamilyMetricsHelper.MetricsEvent.AutoAddedToArchive.name(),
                            CustomerMetricsHelper.getExtraEventTag(MetricTag.MobileAll),
                            addedToFA);
                }

                GLogger.v(TAG, "Ending Upload metrics");
                session.onServiceStopped();

                if (uploadNotificationContainer != null) {
                    uploadNotificationContainer.onUploadServiceNotRunning();
                }

                PhotosApplication.getAppComponent().getGalleryFileUploadProgressStatus()
                        .fileUploadProgressData.clear();
            }

            previousState = state;
        }
    }

    /**
     *  Factory class to generate instances of {@code UnifiedUploadNotificationContainer},
     *  that are pre-registered with the Otto global bus.
     *
     *  There are occasions when the service gets destroyed prematurely, before the last
     *  notification update(computed asynchronously in {@code GalleryUploadStatusTracker})
     *  is published to the instance of {@code UnifiedUploadNotificationContainer}.
     *  Thus we need the instance of {@code UnifiedUploadNotificationContainer}, to outlast
     *  the lifecycle of the service, so that it can receive updates even if the service
     *  gets destroyed and GC'ed.
     */
    private static class OttoAwareUnifiedUploadNotificationContainer {

        private static UnifiedUploadNotificationContainer currentRef;

        @MainThread
        public static UnifiedUploadNotificationContainer create(Context context, Profiler profiler) {
            if (currentRef != null) {
                currentRef.unregisterReceivers();
            }

            currentRef = new UnifiedUploadNotificationContainer(context, profiler);
            currentRef.registerReceivers();
            return currentRef;
        }

    }

    enum MetricEvent {
        InvalidUploadRequestState,
        InvalidRequestFileNotFound,
        InvalidRequestMediaItemNotFound,
        InvalidRequestMultipleItemWithSamePath,
        InvalidRequestItemExists,
    }
}