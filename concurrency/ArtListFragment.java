package concurrency;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.database.Cursor;
import android.graphics.drawable.Drawable;
import android.net.Uri;
import android.os.Bundle;
import android.text.TextUtils;
import android.view.inputmethod.InputMethodManager;
import android.widget.AbsListView;
import android.widget.BaseAdapter;
import com.amazon.mp3.AmazonApplication;
import com.amazon.mp3.R;
import com.amazon.mp3.download.MusicDownloader;
import com.amazon.mp3.library.adapter.BadgeableListAdapter;
import com.amazon.mp3.library.cache.image.CacheManager;
import com.amazon.mp3.library.cache.image.IArtCache;
import com.amazon.mp3.library.cache.image.loader.ImageLoaderFactory;
import com.amazon.mp3.library.dialog.NetworkErrorDialog.INetworkErrorDialogHandler;
import com.amazon.mp3.playback.activity.NowPlayingUtil;
import com.amazon.mp3.playback.playbackcontrol.PlaybackControllerProvider;
import com.amazon.mp3.service.ClearCacheService;
import com.amazon.mp3.service.job.Job;
import com.amazon.mp3.sports.LiveSoccerIndicator;
import com.amazon.mp3.util.ConnectivityUtil;
import com.amazon.mp3.util.Log;
import com.amazon.mp3.util.ScreenUtil;
import com.amazon.music.downloads.notification.NotificationInfo;
import com.amazon.music.events.NavigationAppEvent;
import com.amazon.music.media.playback.ControlSource;
import com.amazon.music.media.playback.MediaCollectionInfo;
import com.amazon.music.media.playback.MediaItem;
import com.amazon.music.media.playback.PlaybackController;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class ArtListFragment extends CursorListFragment implements IArtCache {
    private final static String TAG = ArtListFragment.class.getSimpleName();
    private final static int SCROLL_SETTLE_SHORT_MILLIS = 75;
    protected final static int SCROLL_SETTLE_LONG_MILLIS = 1000;
    private final static int PREFETCH_DELAY_MILLIS = 500;
    private static final int MAX_SCROLL_SPEED = 8000;
    private static final int FAST_SCROLL_VISIBLE_MILLIS = 1000;

    protected PlaybackController mPlaybackController = PlaybackControllerProvider.getController(ControlSource.APP_UI);
    private MediaItem mSavedMediaItem;
    private boolean mSavedIsPlaying;
    private MediaCollectionInfo mSavedMediaCollectionInfo;

    private boolean mResetCacheOnResume;
    protected boolean mListScrolling = false;
    private int mFirstVisible = 0;
    private int mLastVisible = 0;
    protected int mAlbumArtSize;
    private long mPrefetchArtJob = Job.INVALID;
    private boolean mPrefetchComplete;
    private int mLastPrefetchPosition;
    private boolean mFastScrollActive = false;
    private boolean mFastScrollVisible = false;
    private int mPreviousFirstVisibleItem = Integer.MIN_VALUE;

    /**
     * Used to hide soft keyboard when user scrolling the results.
     */
    private InputMethodManager mImm;

    @Override
    public void onCreate(Bundle instanceState) {
        super.onCreate(instanceState);

        registerReceiver(
            mCacheClearedReceiver,
            new IntentFilter(ClearCacheService.ACTION_CACHE_CLEARED)
        );

        mediaCollectionUpdated();
        playStateUpdated();
    }

    @Override
    public void onDestroy() {
        unregisterReceiver(mCacheClearedReceiver);
        super.onDestroy();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onDeactivated() {
        super.onDeactivated();
        unregisterReceiver(mArtLoadedReceiver);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onActivated() {
        super.onActivated();

        registerReceiver(
            mArtLoadedReceiver,
            new IntentFilter(CacheManager.ACTION_IMAGE_CACHED)
        );

        if (mResetCacheOnResume) {
            checkedResetCache();
            mResetCacheOnResume = false;
        }

        if (albumArtCacheEnabled()) {
            getAlbumArtCache().unpause();
        }

        sendUiPageViewMetric();

        init();
    }

    abstract protected @Nullable String getUiPageViewId();

    public void sendUiPageViewMetric() {
        String id = getUiPageViewId();

        if (!TextUtils.isEmpty(id)) {
            sendUiPageViewMetric(id);
        }
    }

    public void sendUiPageViewMetric(final String identifier) {
        sendUiPageViewMetric(identifier, null, null, null, null);
    }

    public void sendUiPageViewMetric(final String identifier, final String itemIdentifier, final String itemIdentifierType) {
        sendUiPageViewMetric(identifier, itemIdentifier, itemIdentifierType, null, null);
    }

    public void sendUiPageViewMetric(final String identifier, final String itemIdentifier, final String itemIdentifierType,
                                     final String refMarker, final String associateTag) {
        NavigationAppEvent.builder(identifier)
            .withOrientation(ScreenUtil.getScreenOrientation(getContext()))
            .withItemIdentifier(itemIdentifier)
            .withItemIdentifierType(itemIdentifierType)
            .withRefMarker(refMarker)
            .withAssociateTag(associateTag)
            .publish();
    }

    @Override
    public void onScroll(AbsListView view, int firstVisibleItem, int visibleItemCount, int totalItemCount) {
        if (firstVisibleItem == mPreviousFirstVisibleItem || visibleItemCount <= 0) {
            return;
        }

        // We use onScroll to work around bugs where onScrollStateChanged doesn't get
        // the SCROLL_STATE_IDLE state when scrolling stops.
        super.onScroll(view, firstVisibleItem, visibleItemCount, totalItemCount);
        if (getAdapter() instanceof BadgeableListAdapter) {
            BadgeableListAdapter adapter = (BadgeableListAdapter) getAdapter();
            adapter.onScroll(firstVisibleItem, visibleItemCount, totalItemCount);
        }

        mHandler.removeCallbacks(mOnScrollSettledRunnable);
        mFirstVisible = firstVisibleItem;
        mHandler.postDelayed(mOnScrollSettledRunnable, SCROLL_SETTLE_LONG_MILLIS);

        mPreviousFirstVisibleItem = firstVisibleItem;
    }

    @Override
    public void onScrollStateChanged(AbsListView view, int scrollState) {
        super.onScrollStateChanged(view, scrollState);
        if (scrollState != SCROLL_STATE_IDLE) {
            mHandler.removeCallbacks(mOnScrollSettledRunnable);

            if (!mListScrolling) {
                mListScrolling = true;
                // getAlbumArtCache().pause();
            }
            LiveSoccerIndicator.hide();
        } else {
            // In this case we're confident that scrolling has stopped, so resume
            // the art cache after a short delay.
            mHandler.postDelayed(mOnScrollSettledRunnable, SCROLL_SETTLE_SHORT_MILLIS);
            mFirstVisible = view.getFirstVisiblePosition();
            mLastVisible = view.getLastVisiblePosition();
            LiveSoccerIndicator.show();
        }
        if (!mFastScrollActive) {
            return;
        }
        if (scrollState == SCROLL_STATE_IDLE) {
            if (mFastScrollVisible) {
                mHandler.postDelayed(mDelayedFastScrollFixRunnable, FAST_SCROLL_VISIBLE_MILLIS);
            }
        } else {
            mHandler.removeCallbacks(mDelayedFastScrollFixRunnable);
            view.setFastScrollEnabled(true);
            view.setFastScrollAlwaysVisible(true);
            mFastScrollVisible = true;

            //hide soft keyboard when item is clicked
            if (mImm != null) {
                mImm.hideSoftInputFromWindow(view.getWindowToken(), 0);
            }
        }
    }

    /**
     * @return the size (in raw pixels) of the album art images that will
     * be displayed. It is important to specify the correct value here, as
     * it will be used to determine the cache size.
     */
    protected int getAlbumArtSize() {
        if (mAlbumArtSize == 0) {
            mAlbumArtSize = mUseGridView
                ? getResources().getDimensionPixelSize(R.dimen.default_gridview_art_size)
                : getResources().getDimensionPixelSize(R.dimen.default_listview_art_size);
        }

        return mAlbumArtSize;
    }

    /**
     * @return a reference to a concrete CacheManager. This instance
     * will be automatically bound to certain Activity lifecycle and ListView
     * scroll events for optimized scrolling performance and memory
     * consumption.
     */
    protected abstract CacheManager getAlbumArtCache();

    /**
     * @return true if the AlbumArtCache should be enabled, false otherwise.
     */
    protected boolean albumArtCacheEnabled() {
        return true;
    }

    private void init() {
        setGridViewMaximumSpeed(MAX_SCROLL_SPEED);
        mImm = (InputMethodManager) getActivity().getSystemService(Context.INPUT_METHOD_SERVICE);
    }

    private void setGridViewMaximumSpeed(int maxSpeed) {
        Exception failedWithException = null;
        try {
            Field field = AbsListView.class.getDeclaredField("mMaximumVelocity");
            field.setAccessible(true);
            field.set(getListView(), Integer.valueOf(maxSpeed));
        } catch (NoSuchFieldException e) {
            failedWithException = e;
        } catch (IllegalArgumentException e) {
            failedWithException = e;
        } catch (IllegalAccessException e) {
            failedWithException = e;
        }

        if (failedWithException != null) {
            Log.info(TAG, "Failed to limit library fling velocity: ", failedWithException);
        }
    }

    private void checkedResetCache() {
        CacheManager cache = getAlbumArtCache();

        if (cache != null) {
            cache.reset();

            BaseAdapter adapter = getAdapter();

            if (adapter != null) {
                adapter.notifyDataSetChanged();
            }
        }
    }

    private Runnable mStartPrefetchRunnable = new Runnable() {
        @Override
        public void run() {
            startPrefetch();
        }
    };

    protected Runnable mOnScrollSettledRunnable = new Runnable() {
        @Override
        public void run() {
            mListScrolling = false;

            // When album art cache is a stack instead of a queue for handling requests, we no longer
            // need to clear - and if we no longer need to clear, we no longer need to notify dataset changed
            if (albumArtCacheEnabled() && !isPaused()) {
                getAlbumArtCache().unpause();
                startPrefetch();
            }

            BaseAdapter adapter = getAdapter();

            if (adapter != null) {
                adapter.notifyDataSetChanged();
            }
        }
    };

    private BroadcastReceiver mCacheClearedReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            if (isPaused()) {
                mResetCacheOnResume = true;
                return;
            }

            checkedResetCache();
        }
    };

    private BroadcastReceiver mArtLoadedReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            onArtReceived(context, intent);
        }
    };

    protected void onArtReceived(Context context, Intent intent) {
        if (getActivity() == null) {
            return;
        }

        if (intent == null || intent.getExtras() == null) {
            return;
        }

        if ((getImageType().intValue() == intent.getIntExtra(CacheManager.EXTRA_IMAGE_TYPE, -1)) &&
            (getAlbumArtSize() == intent.getIntExtra(CacheManager.EXTRA_IMAGE_SIZE, -1))) {
            boolean loaded = false;
            String source = intent.getStringExtra(CacheManager.EXTRA_IMAGE_SOURCE);
            String idStr = intent.getStringExtra(CacheManager.EXTRA_IMAGE_ID);

            if (idStr == null) {
                return;
            }

            if (getAdapter() instanceof IArtLoadedListener) {

                IArtLoadedListener listener = (IArtLoadedListener) getAdapter();
                // idStr can contain an Asin (alphanumeric) or an albumId (all numeric)
                if (TextUtils.isDigitsOnly(idStr)) {
                    long id = Long.parseLong(idStr);
                    Drawable d = getDrawable(getImageType(), source, id);
                    if (d != null) {
                        listener.onDrawableLoaded(d, source, id);
                        loaded = true;
                    }
                } else {
                    Drawable d = getDrawable(getImageType(), source, idStr);
                    if (d != null) {
                        listener.onDrawableLoaded(d, source, idStr);
                        loaded = true;
                    }
                }
            }

            if (!loaded) {
                Log.warning(
                    TAG,
                    "Can not load image type [%s] with id of [%s]",
                    getImageType(), idStr
                );
            }
        }
    }

    @Override
    protected void onCursorChanged(Cursor newCursor, Cursor oldCursor) {
        super.onCursorChanged(newCursor, oldCursor);
        mPrefetchComplete = false;
        mLastPrefetchPosition = 0;

        // onCursorChanged() is called in onDestroy(). If we start a job here it will create a new session for
        // a fragment that is no longer in use, causing a memory leak. We can assume that we don't need to start up
        // the job if the new cursor is null
        if (mListScrolling == false && newCursor != null) {
            // start prefetch once we've had a chance to handle all the bindView calls for the
            // current visible items.
            mHandler.postDelayed(mStartPrefetchRunnable, PREFETCH_DELAY_MILLIS);
        }
    }

    protected void startPrefetch() {
        // Don't start a prefetch if we have detached from the activity. This can happen because this method is called
        // from a handler with a delay.
        if (getActivity() != null && shouldPrefetchArt() && mPrefetchArtJob == Job.INVALID) {
            mPrefetchArtJob = addJob(new PrefetchArtJob());
        }
    }

    private class PrefetchArtJob extends Job {

        ConcurrentLinkedQueue<String> mImageQueue = new ConcurrentLinkedQueue<String>();

        public PrefetchArtJob() {
            int count = 0;
            int index = 0;
            int first = Math.max(mLastVisible, mLastPrefetchPosition);

            if (!AmazonApplication.onMainThread()) {
                Log.error(TAG, "PrefetchArtJob not on main thread");
                throw new IllegalArgumentException();
            }
            Cursor cursor = getCursor();
            if (cursor != null) {
                count = cursor.getCount();
                int remaining = count - first;
                if (remaining <= 0) {
                    mPrefetchComplete = true;
                } else {
                    remaining = Math.min(remaining, CacheManager.getMaxPrefetchCount());
                    index = cursor.getColumnIndexOrThrow(getArtColumnName());
                    cursor.moveToPosition(first);
                    mLastPrefetchPosition = first;
                    while (remaining-- > 0) {
                        final String artId = cursor.getString(index);
                        if (TextUtils.isEmpty(artId) == false) {
                            // make sure we skip over empty strings.
                            mImageQueue.add(artId);
                        }
                        if (!cursor.moveToNext()) {
                            remaining = 0; // short circuit if end of cursor
                        }
                    }
                }
            }
        }

        @Override
        public int run() throws Exception {
            prefetchArt(mImageQueue);
            return RESULT_SUCCESS;
        }
    }

    protected abstract String getArtColumnName();

    protected abstract ImageLoaderFactory.ItemType getImageType();

    protected boolean shouldPrefetchArt() {
        // Only prefetch if not using SICS.
        return true;
    }

    private void prefetchArt(ConcurrentLinkedQueue<String> queue) {
        if (mPrefetchComplete) {
            return;
        }

        int artSize = getAlbumArtSize();

        CacheManager cache = CacheManager.getInstance();
        while (!queue.isEmpty() && !mListScrolling) {
            final String id = queue.poll();
            cache.precache(getImageType(), null, artSize, id);
            mLastPrefetchPosition++;
        }
    }

    @Override
    protected void onJobFinished(long jobId, Job job, int resultCode, Bundle resultBundle) {
        if (jobId == mPrefetchArtJob) {
            mPrefetchArtJob = Job.INVALID;
        }
        super.onJobFinished(jobId, job, resultCode, resultBundle);
    }

    @Override
    public Drawable getDrawable(ImageLoaderFactory.ItemType itemType, String source, String id) {
        return getAlbumArtCache().get(itemType, source, getAlbumArtSize(), id);
    }

    @Override
    public Drawable getDrawable(ImageLoaderFactory.ItemType itemType, String source, long id) {
        return getAlbumArtCache().get(itemType, source, getAlbumArtSize(), Long.toString(id));
    }

    @Override
    public void precacheDrawable(ImageLoaderFactory.ItemType itemType, String source, long id) {
        getAlbumArtCache().precache(itemType, source, getAlbumArtSize(), Long.toString(id));
    }

    protected final void startDownload(final String requestId, final Uri contentUri,
                                       final boolean downloadNonAddedPrimeTracks,
                                       final NotificationInfo notificationInfo) {
        if (!ConnectivityUtil.hasAnyInternetConnection()) {
            showNetworkErrorDialog(
                new INetworkErrorDialogHandler() {
                    @Override
                    public void onRetryConnection() {
                        startDownload(
                            requestId, contentUri, downloadNonAddedPrimeTracks, notificationInfo);
                    }

                    @Override
                    public void onConnectionErrorDialogCancel() {
                        // ignore.
                    }

                    @Override
                    public void onDialogDismiss() {
                        // ignore.
                    }
                });

            return;
        }

        if (DownloadSettingDialogFragment.shouldShowDownloadSettingsDialogFragment(getActivity())) {
            DownloadSettingDialogFragment.showSettingsDialog(
                getActivity(), requestId, contentUri, notificationInfo, downloadNonAddedPrimeTracks);
            return;
        }

        MusicDownloader.getInstance(getApplication()).download(
            requestId, contentUri, notificationInfo, downloadNonAddedPrimeTracks, true);
    }

    protected void cancelDownload(String requestId) {
        MusicDownloader.getInstance(getApplication()).cancel(requestId);
    }

    /**
     * Check if FastScroller is required based on the size of cursor and enable it. For
     * Gingerbread and lower, always enable fast scroller
     * #DMM-4851, which happens on Gingerbread devices when fast scroller is
     * switched between false and true.
     *
     * @param cursor Cursor to check if fast scroller required
     * @param upperLimit Maximum number of rows before we switch fast scroller
     */
    protected void checkFastScrollerRequired(Cursor cursor, int upperLimit) {
        // null check added for DMM-6101
        final AbsListView view = getListView();
        if (view != null && shouldUseFastScroll()) {
            monitorScrolling((cursor != null) && (cursor.getCount() > upperLimit));
        }
    }

    private void monitorScrolling(boolean monitorScroll) {
        mFastScrollActive = monitorScroll;
    }

    private Runnable mDelayedFastScrollFixRunnable = new Runnable() {
        @Override
        public void run() {
            final AbsListView view = getListView();
            if (view != null && mFastScrollVisible) {
                view.setFastScrollEnabled(false);
                view.setFastScrollAlwaysVisible(false);
                mFastScrollVisible = false;
            }
        }
    };

    /**
     * base method for classes to override if they never want to use fast scroll
     *
     * @return Whether or not fast scroll should ever be used
     */
    protected boolean shouldUseFastScroll() {
        return true;
    }

    protected boolean playStateUpdated() {
        final boolean shouldUpdate = NowPlayingUtil.isPlayingChanged(mSavedMediaItem, mSavedIsPlaying);
        mSavedMediaItem = mPlaybackController.getCurrentMediaItem();
        mSavedIsPlaying = mPlaybackController.getPlayStatus().shouldBePlaying();
        return shouldUpdate;
    }

    protected boolean mediaCollectionUpdated() {
        final boolean mediaCollectionInfoChanged = mSavedMediaCollectionInfo != mPlaybackController.getMediaCollectionInfo();
        mSavedMediaCollectionInfo = mPlaybackController.getMediaCollectionInfo();
        return mediaCollectionInfoChanged;
    }
}