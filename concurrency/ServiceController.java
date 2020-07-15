/*
 * Copyright (C) 2008-2009 Marc Blank
 * Licensed to The Android Open Source Project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * ServiceController.java
 *
 * Copyright (c) 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */

package concurrency;

import android.app.ActivityManager;
import android.app.ActivityManager.RunningAppProcessInfo;
import android.app.AlarmManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.*;
import android.database.ContentObserver;
import android.database.Cursor;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.net.NetworkInfo.State;
import android.net.Uri;
import android.os.*;
import android.provider.CalendarContract;
import android.provider.ContactsContract;
import android.util.LongSparseArray;
import com.amazon.email.utils.ExceptionStateManager;
import com.amazon.email.utils.SecurityPolicy;
import com.amazon.pim.logging.Logging;
import com.amazon.pim.platform.DemoModeHelper;
import com.android.email.NotificationController;
import com.android.email.activity.setup.AccountSettingsUtils;
import com.android.email.activity.setup.AccountSetupController;
import com.android.email.provider.EmailProvider;
import com.android.email.service.ServiceUtils.ServiceInfo;
import com.android.email.util.AccountUtils;
import com.android.email.util.MessageUtils;
import com.android.emailcommon.EmailLogging;
import com.android.emailcommon.TempDirectory;
import com.android.emailcommon.mail.MessagingException;
import com.android.emailcommon.provider.*;
import com.android.emailcommon.provider.EmailContent.Attachment;
import com.android.emailcommon.provider.EmailContent.MailboxColumns;
import com.android.emailcommon.provider.EmailContent.Message;
import com.android.emailcommon.provider.EmailContent.MessageColumns;
import com.android.emailcommon.service.*;
import com.android.emailcommon.service.ServiceProxy.ServiceProxyCallback;
import com.android.emailcommon.sync.DeliveryMetrics;
import com.android.emailcommon.utility.Utility;
import com.google.common.primitives.Longs;

import java.io.FileDescriptor;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ServiceController handles the lifecycle of various sync adapters used by services that
 * cannot rely on the system SyncManager
 *
 * ServiceController uses ContentObservers to detect changes to accounts, mailboxes, & messages in
 * order to maintain proper 2-way syncing of data.  (More documentation to follow)
 *
 */
public final class ServiceController extends Service implements Runnable {


    private static String TAG = ServiceController.class.getSimpleName();

    // The email sync process name ends with this suffix.  This must match the ProtocolService
    // definitions in AndroidManifest.xml
    private static final String SYNC_PROCESS_SUFFIX = ":Sync";

    private static final int SECONDS = 1000;
    private static final int MINUTES = 60*SECONDS;
    private static final int ONE_DAY_MINUTES = 1440;

    // Watchdog delay in case we become uninterruptable (belt & suspenders)
    private static final int SERVICE_CONTROLLER_HEARTBEAT_TIME = 15*MINUTES;
    // Watchdog delay in case a connectivity wait becomes uninterruptable (belt & suspenders)
    private static final int CONNECTIVITY_WAIT_TIME = 30*MINUTES;
    // Delay after a change to a syncable message before we send the change to the server
    private static final int UPSYNC_DELAY_TIME = 10*SECONDS;

    // Min time to runAsleep, otherwise we will just wait
    private static final int MIN_SLEEP_TIME = 10 * SECONDS;
    private static final int SLEEP_EXTRA_TIME = 3 * SECONDS;
    private static final int MIN_WAIT_TIME = 1 * SECONDS;

    // Sync hold constants for services with transient errors
    private static final int HOLD_DELAY_MINIMUM = 15*SECONDS;
    private static final int HOLD_DELAY_MAXIMUM = 4*MINUTES;

    public static final int IDLE_FALLBACK_SYNC_INTERVAL = 15; // 15 minutes

    // Reason codes when SyncServiceManager.kick is called (mainly for debugging)
    public static final int SYNC_NONE = -1;
    // UI has changed data, requiring an upsync of changes
    public static final int SYNC_UPSYNC = 0;
    // A scheduled sync (when not using push)
    public static final int SYNC_SCHEDULED = 1;
    // Mailbox was marked push
    public static final int SYNC_PUSH = 2;
    // A meeting response needs to be sent to the server
    public static final int SYNC_SEND_MEETING_RESPONSE = 3;
    // Misc. reason
    public static final int SYNC_REASON_ANY = 4;
    // Request to stop
    public static final int SYNC_STOP = 5;
    //startSync was requested via refresh manager.
    public static final int SYNC_AUTO_REFRESH = 6;
    //startSync was requested to send all messages as a background request.
    public static final int SYNC_SEND_ALL = 7;
    // startSync was requested via background attachment download service.
    public static final int SYNC_LOAD_ATTACHMENT = 8;

    public static final int SYNC_UI_FOREGROUND = 9;
    // startSync was requested via "Refresh"
    public static final int SYNC_UI_REFRESH = SYNC_UI_FOREGROUND;
    // startSync was requested via "Load Full Message"
    public static final int SYNC_UI_LOAD_FULL_MESSAGE = SYNC_UI_FOREGROUND + 1;
    // startSync was requested via "Load Attachment"
    public static final int SYNC_UI_LOAD_ATTACHMENT = SYNC_UI_FOREGROUND + 2;
    //startSync was requested via "Load more messages"
    public static final int SYNC_LOAD_MORE = SYNC_UI_FOREGROUND + 3;

    //startSync was requested for Drafts
    public static final int SYNC_NEW_DRAFT = 20;

    private static final String[] SYNC_REASONS = new String[] {
        "Upsync", "Scheduled", "Push", "Meeting", "Any", "Stop", "AutoRefresh", "SendAll",
        "LoadAttachment", "Refresh", "LoadFullMessage", "UILoadAttachment", "LoadMore"
    };

    // Mailboxes that we can run via ServiceController: "push", "outbox" (if mail present), and
    // "scheduled" (interval >= 5)
    // For supporting EAS 16 we query the Drafts mailbox to sync Drafts
    private static final String WHERE_OUTBOX_SENT_DRAFTS_PUSH_OR_SCHEDULED =
        "(" + MailboxColumns.TYPE + " IN (" + Mailbox.TYPE_OUTBOX + "," + Mailbox.TYPE_INBOX +
        "," + Mailbox.TYPE_SENT + "," + Mailbox.TYPE_DRAFTS +
        ") OR " + MailboxColumns.SYNC_INTERVAL + "=" + Mailbox.CHECK_INTERVAL_PUSH +
        " OR " + MailboxColumns.SYNC_INTERVAL + ">=5)";

    private static final String WHERE_MAILBOX_KEY_AND_DRAFT_MESSAGE = MessageColumns.MAILBOX_KEY
            + "=? and " + MessageColumns.FLAGS + "&" + Message.FLAG_DRAFTS_PENDING_SYNC + "="
            + Message.FLAG_DRAFTS_PENDING_SYNC;

    // Offsets into the syncStatus data for EAS that indicate type, exit status, and change count
    // The format is S<type_char>:<exit_char>:<change_count>
    public static final int STATUS_TYPE_CHAR = 1;
    public static final int STATUS_EXIT_CHAR = 3;
    public static final int STATUS_CHANGE_COUNT_OFFSET = 5;

    // We synchronize on this for all actions affecting the service and error maps
    private static final Object sControllerLock = new Object();
    // We synchronize on this for all actions affecting the service and error maps
    private static final Object sWaitLock = new Object();
    // All threads can use this lock to wait for connectivity
    private static final Object sConnectivityLock = new Object();
    private static boolean sConnectivityHold = false;

    // Keeps track of running services (by mailbox id)
    private final ConcurrentHashMap<Long, ServiceEntry> mServiceMap =
        new ConcurrentHashMap<Long, ServiceEntry>();
    // Cache of service proxies
    private final ConcurrentHashMap<String, EmailServiceProxy> mProxyCache =
        new ConcurrentHashMap<String, EmailServiceProxy>();

    // Observers that we use to look for changed mail-related data
    private final Handler mHandler = new Handler();
    private MailboxObserver mMailboxObserver;
    private ArrayList<AccountUpdateObserver> mAccountUpdateObservers =
        new ArrayList<AccountUpdateObserver>();
    private AccountInsertObserver mAccountInsertObserver;
    private SyncedMessageObserver mSyncedMessageObserver;

    public ContentResolver mResolver;

    // The singleton SyncServiceManager object, with its thread and stop flag
    private static volatile ServiceController sInstance;
    private static volatile Thread sServiceThread = null;
    // Cached unique device id
    private static PowerHandler sPowerHandler;
    private static ConnectivityManager sConnectivityManager;
    // Whether we have an unsatisfied "kick" pending
    private static volatile boolean sKicked = false;
    private static volatile boolean sStop = false;

    // The reason for SyncServiceManager's next wakeup call
    private String mNextWaitReason;

    // Receiver of connectivity broadcasts
    private ConnectivityReceiver mConnectivityReceiver = null;
    private ConnectivityReceiver mBackgroundDataSettingReceiver = null;
    private volatile boolean mBackgroundData = true;
    // The most current NetworkInfo (from ConnectivityManager)
    private NetworkInfo mNetworkInfo;

    // Callback lists, etc.
    private static final RemoteCallbackList<IEmailServiceCallback> sCallbackList =
        new RemoteCallbackList<IEmailServiceCallback>();
    private final ServiceCallback mEmailCallback = new ServiceCallback(sCallbackList);

    // Keep track of which syncs are in progress, based on callbacks
    private final ConcurrentHashMap<Long, Boolean> mInProgressMailboxMap =
        new ConcurrentHashMap<Long, Boolean>();
    // Keep track of the manager of managed mailboxes
    private ConcurrentHashMap<Long, Long> mManagedMailboxMap = new ConcurrentHashMap<Long, Long>();

    public static ServiceController getInstance() {
        return sInstance;
    }

    private static class MailboxObserver extends ContentObserver {
        public MailboxObserver(Handler handler) {
            super(handler);
        }

        @Override
        public void onChange(boolean selfChange) {
            // See if there's anything to do...
            if (!selfChange) {
                Logging.log("Mailbox notifier kick...");
                kick("Mailbox notifier");
            }
        }
    }

    private void registerAccountObservers() {
        mAccountInsertObserver = new AccountInsertObserver(mHandler);
        new Account.ForEachAccount(ServiceController.this) {
            @Override
            public void execute(long accountId) {
                mAccountUpdateObservers.add(new AccountUpdateObserver(mHandler, accountId));
            }}.run();
    }

    private void unregisterAccountObservers() {
        ContentResolver cr = getContentResolver();
        if (mAccountInsertObserver != null) {
            cr.unregisterContentObserver(mAccountInsertObserver);
            mAccountInsertObserver = null;
        }
        for (AccountUpdateObserver obs: mAccountUpdateObservers) {
            cr.unregisterContentObserver(obs);
        }
        mAccountUpdateObservers.clear();
    }

    private class AccountUpdateObserver extends ContentObserver {
        private long mId;
        private int mFlags;
        private int mSyncInterval;
        private int mSyncLookback;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof AccountUpdateObserver)) {
                return false;
            }
            AccountUpdateObserver obs = (AccountUpdateObserver)o;
            return (obs.mId == mId);
        }

        @Override
        public int hashCode() {
            return (int)mId;
        }

        public AccountUpdateObserver(Handler handler, long id) {
            super(handler);
            Account acct = Account.restoreAccountWithId(ServiceController.this, id);
            if (acct != null) {
                mId = acct.mId;
                saveState(acct);
                Uri uri = Account.NOTIFIER_URI.buildUpon()
                    .appendPath(EmailProvider.NOTIFICATION_OP_UPDATE)
                    .appendPath(Long.toString(id)).build();
                getContentResolver().registerContentObserver(uri, false, this);
            }
        }

        private void saveState(Account acct) {
            mFlags = acct.mFlags;
            mSyncInterval = acct.mSyncInterval;
            mSyncLookback = acct.mSyncLookback;
        }

        @Override
        public void onChange(boolean selfChange) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    Account acct = Account.restoreAccountWithId(ServiceController.this, mId);
                    if (acct == null) {
                        // TODO The account has been deleted; do something!
                        return;
                    }
                    long inboxId = Mailbox.findMailboxOfType(ServiceController.this, acct.mId,
                        Mailbox.TYPE_INBOX);
                    if (inboxId == Mailbox.NO_MAILBOX) return;

                    if ((mFlags & Account.FLAGS_HIDDEN) != 0) {
                        if ((acct.mFlags & Account.FLAGS_HIDDEN) == 0) {
                            Logging.warn(TAG, "Hidden flag cleared for " + acct.mId);
                            releaseSyncHolds(ServiceController.this,
                                EmailServiceStatus.SECURITY_FAILURE, acct);
                        }
                    }
                    int newInterval = acct.mSyncInterval;
                    if (mSyncInterval != newInterval) {
                        Logging.log(TAG, "Sync interval changed for ", acct.mId);
                        if (mSyncInterval == Account.CHECK_INTERVAL_PUSH ||
                                newInterval == Account.CHECK_INTERVAL_PUSH) {
                            // From push -> timed or vice versa, stop long-running inbox syncs
                            ContentValues values = new ContentValues();
                            int inboxInterval = newInterval;
                            // If the account has a manager, use MANAGED instead of PUSH
                            if (newInterval == Account.CHECK_INTERVAL_PUSH &&
                                acct.isManaged(ServiceController.this)) {
                                inboxInterval = Mailbox.CHECK_INTERVAL_MANAGED;
                            }
                            values.put(Mailbox.SYNC_INTERVAL, inboxInterval);
                            // TODO Clean up/document account vs inbox sync interval
                            mResolver.update(
                                ContentUris.withAppendedId(Mailbox.CONTENT_URI, inboxId),
                                values, null, null);
                            long sentId = Mailbox.findMailboxOfType(ServiceController.this,
                                acct.mId, Mailbox.TYPE_SENT);
                            // Sent should now always match Inbox
                            if (sentId != Mailbox.NO_MAILBOX) {
                                mResolver.update(
                                    ContentUris.withAppendedId(Mailbox.CONTENT_URI, sentId),
                                    values, null, null);
                            }
                            stopAccountSyncs(mId);
                            kick("Sync interval changed");
                        }
                    }
                    if (mSyncLookback != acct.mSyncLookback) {
                        Logging.log(TAG, "Sync lookback changed from " + mSyncLookback + " to " +
                            acct.mSyncLookback + " for ", acct.mId);
                        startSync(ServiceController.this, inboxId,
                            ServiceController.SYNC_UI_REFRESH);
                        acct.clearInitialSyncComplete(ServiceController.this);
                    }
                    saveState(acct);
                }}).start();
        }
    }

    private class AccountInsertObserver extends ContentObserver {
        public AccountInsertObserver(Handler handler) {
            super(handler);
            Uri uri = Account.NOTIFIER_URI.buildUpon()
                .appendPath(EmailProvider.NOTIFICATION_OP_INSERT).build();
            getContentResolver().registerContentObserver(uri, false, this);
        }

        @Override
        public void onChange(boolean selfChange) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    new Account.ForEachAccount(ServiceController.this) {
                        @Override
                        public void execute(long accountId) {
                            // See if we have an observer
                            for (AccountUpdateObserver obs: mAccountUpdateObservers) {
                                if (obs.mId == accountId) return;
                            }
                            Account acct =
                                Account.restoreAccountWithId(ServiceController.this, accountId);
                            if (acct == null) return;
                            Logging.log(TAG, "Account observer added for ", acct.mId);
                            mAccountUpdateObservers.add(
                                new AccountUpdateObserver(mHandler, accountId));
                        }}.run();
                }}).start();
        }
    }

    private class SyncedMessageObserver extends ContentObserver {
        Intent syncAlarmIntent = new Intent(ServiceController.this, UpsyncAlarmReceiver.class);
        PendingIntent syncAlarmPendingIntent =
            PendingIntent.getBroadcast(ServiceController.this, 0, syncAlarmIntent, 0);
        AlarmManager alarmManager =
            (AlarmManager)ServiceController.this.getSystemService(Context.ALARM_SERVICE);

        public SyncedMessageObserver(Handler handler) {
            super(handler);
        }

        @Override
        public void onChange(boolean selfChange) {
            long alarmTime = System.currentTimeMillis() + UPSYNC_DELAY_TIME;
            alarmManager.set(AlarmManager.RTC_WAKEUP, alarmTime, syncAlarmPendingIntent);
        }
    }

    public static class SyncStatus {
        static public final int NOT_RUNNING = 0;
        static public final int DIED = 1;
        static public final int SYNC = 2;
        static public final int IDLE = 3;
    }

    private static class ServiceEntry {
        private static final int NO_ERROR = -1;

        final long mMailboxId;
        final long mAccountId;
        final int mSyncInterval;
        final String mProtocol;
        int mError = NO_ERROR;
        long mHoldEndTime = 0;
        int mHoldDelay = HOLD_DELAY_MINIMUM;

        private ServiceEntry(Mailbox mailbox, String protocol, int syncInterval) {
            mMailboxId = mailbox.mId;
            mAccountId = mailbox.mAccountKey;
            mProtocol = protocol;
            mSyncInterval = syncInterval;
        }

        /**
         * We double the holdDelay from 15 seconds through 8 mins
         */
        void backoff() {
            mError = EmailServiceStatus.CONNECTION_ERROR;
            if (mHoldDelay <= HOLD_DELAY_MAXIMUM) {
                mHoldDelay *= 2;
            }
            mHoldEndTime = System.currentTimeMillis() + mHoldDelay;
        }

        void resetError() {
            mError = NO_ERROR;
        }
    }

    /**
     * Release a specific type of hold (the reason) for the specified Account; if the account
     * is null, mailboxes from all accounts with the specified hold will be released
     * @param reason the reason for the SyncError (AbstractSyncService.EXIT_XXX)
     * @param account an Account whose mailboxes should be released (or all if null)
     * @return whether or not any mailboxes were released
     */
    private void releaseSyncHolds(Context context, int reason, Account account) {
        boolean released = false;
        long outboxId = Mailbox.NO_MAILBOX;
        synchronized (sControllerLock) {
            for (long mailboxId: mServiceMap.keySet()) {
                Mailbox m = Mailbox.restoreMailboxWithId(context, mailboxId);
                if (m == null) {
                    mServiceMap.remove(mailboxId);
                    released = true;
                    continue;
                }
                ServiceEntry entry = mServiceMap.get(mailboxId);
                boolean reasonMatched = entry.mError == reason;
                if (EmailServiceStatus.LOGIN_FAILED == reason) {
                    // Check for send mail failure too because send failure due to auth error
                    // was mapped to send mail failure.
                    reasonMatched = reasonMatched ||
                            entry.mError == EmailServiceStatus.SEND_MAIL_FAILURE;
                }
                if (reasonMatched && ((account == null) || (account.mId == m.mAccountKey))) {
                    released = true;
                    if (m.mType == Mailbox.TYPE_OUTBOX) {
                        outboxId = mailboxId;
                        // Reset error so checkMailboxes() won't kick off a sync as we'll do
                        // that next.
                        entry.resetError();
                    } else {
                        mServiceMap.remove(mailboxId);
                    }
                }
            }
        }
        if (released) {
            kick("Release sync hold for " + ((account == null) ? "all" : account.mId));
        }
        // If the outbox sync hold was released then start a sync to send all messages.
        if (outboxId != Mailbox.NO_MAILBOX) {
            // Make all messages sendable
            ExceptionStateManager.clearOutboxErrorFlagSync(context, outboxId);
            startSync(this, outboxId, SYNC_SEND_ALL);
        }
    }

    public void stopAccountSyncs(long acctId) {
        synchronized (sControllerLock) {
            List<Long> deletedBoxes = new ArrayList<Long>();
            for (Long mid : mServiceMap.keySet()) {
                Mailbox box = Mailbox.restoreMailboxWithId(this, mid);
                if (box != null && box.mAccountKey == acctId) {
                    try {
                        ServiceEntry entry = mServiceMap.get(mid);
                        if (entry != null) {
                            EmailServiceProxy svc = mProxyCache.get(entry.mProtocol);
                            svc.stopSync(mid);
                        }
                    } catch (RemoteException e) {
                        // We tried
                    }
                    deletedBoxes.add(mid);
                }
            }
            for (Long mid : deletedBoxes) {
                releaseMailbox(mid);
            }
        }
    }

    public class ConnectivityReceiver extends BroadcastReceiver {
        @Override
        public void onReceive(Context context, Intent intent) {
            if (intent.getAction().equals(ConnectivityManager.CONNECTIVITY_ACTION)) {
                Bundle b = intent.getExtras();
                if (b != null) {
                    @SuppressWarnings("deprecation")
                    NetworkInfo a = (NetworkInfo)b.get(ConnectivityManager.EXTRA_NETWORK_INFO);
                    String name = a.getTypeName();
                    String info = "Connectivity alert for " + name;
                    State state = a.getState();
                    if (state == State.CONNECTED) {
                        info += " CONNECTED";
                        Logging.log(TAG, info);
                        synchronized (sConnectivityLock) {
                            sConnectivityLock.notifyAll();
                        }
                        DeliveryMetrics.networkUp(context, name);
                        kick("connected");
                    } else if (state == State.DISCONNECTED) {
                        info += " DISCONNECTED";
                        Logging.log(TAG, info);
                        DeliveryMetrics.networkDown(context, name);
                        kick("disconnected");
                    }
                }
            } else if (intent.getAction().equals(
                ConnectivityManager.ACTION_BACKGROUND_DATA_SETTING_CHANGED)) {
                mBackgroundData = sConnectivityManager.getBackgroundDataSetting();
                // If background data is now on, we want to kick SyncServiceManager
                if (mBackgroundData) {
                    kick("background data on");
                    Logging.log(TAG, "Background data on; restart syncs");
                    // Otherwise, stop all syncs
                } else {
                    Logging.log(TAG, "Background data off: stop all syncs");
                    // FIXME: This call was runAsyncParallel, to reduce rejectedExecutionException
                    // the call is made serial. Refer: PIM-17503
                    EmailAsyncTask.runAsyncSerial(new Runnable() {
                        @Override
                        public void run() {
                            //synchronized (mAccountList) {
                            //    for (Account account : mAccountList)
                            //        Scheduler.stopAccountSyncs(account.mId);
                           //}
                        }});
                }
            }
        }
    }

    private void startSyncFromCursor(Cursor c) {
        Mailbox m = EmailContent.getContent(c, Mailbox.class);
        startSync(this, m, SYNC_SCHEDULED);
    }

    private void startSync(Context context, Mailbox mailbox, int reason) {
        if (DemoModeHelper.getInstance(this).isDemo() || sConnectivityHold || (mailbox == null) || sStop) {
            return;
        }
        try {
            synchronized(sControllerLock) {
                Account acct = Account.restoreAccountWithId(this, mailbox.mAccountKey);
                if (acct == null) return;
                ServiceEntry entry = mServiceMap.get(mailbox.mId);
                if (entry != null && entry.mError != ServiceEntry.NO_ERROR) {
                    switch(reason) {
                        case SYNC_SCHEDULED:
                        case SYNC_PUSH:
                            // ServiceController shouldn't do this...
                            throw new IllegalStateException("Restarting service in error state?");
                        case SYNC_UPSYNC:
                            // At least report auth error.
                            if (entry.mError == EmailServiceStatus.LOGIN_FAILED) {
                                mEmailCallback.syncMailboxStatus(mailbox.mId, entry.mError, null,
                                        true);
                            }
                            // Don't bother if we're in an error state; we'll start as appropriate
                            return;
                        default:
                            // Keep going...
                    }
                }
                // If the service is running, there's nothing to do
                EmailServiceProxy service = getPersistentServiceForMailbox(context, mailbox);
                if (service != null) {
                    service.startSync(mailbox.mId, reason);
                } else {
                    Logging.error(TAG, "startSync failed; no service!");
                }
            }
        } catch (RemoteException e) {
            // Remote service is dead; it will restart on its own
        }
    }

    private static class EmailServiceProxyCallback implements ServiceProxyCallback {
        private final String mProtocol;
        private final ServiceController mServiceController;
        private final EmailServiceProxy mProxy;

        private EmailServiceProxyCallback(ServiceController serviceController,
            EmailServiceProxy proxy, String protocol) {
            mServiceController = serviceController;
            mProtocol = protocol;
            mProxy = proxy;
        }

        @Override
        public void onDisconnect(ComponentName name) {
            Logging.error(TAG, "EmailServiceProxy disconnected: " + mProxy);
            // Clean up thread information, etc.
            new Thread(new Runnable() {
                @Override
                 public void run() {
                     mServiceController.cleanupDisconnectedService(mProtocol);
                 }}).start();
            DeliveryMetrics.serviceDown(sInstance, mProtocol);
        }
    }

    private void cleanupDisconnectedService(String protocol) {
        synchronized (sControllerLock) {
            for (long mid: mServiceMap.keySet()) {
                ServiceEntry entry = mServiceMap.get(mid);
                if (entry != null && entry.mProtocol.equals(protocol)) {
                    Logging.warn(TAG, "Removing " + protocol + " from service map");
                    mServiceMap.remove(mid);
                }
            }
            // Remove the protocol from the cache; the proxy is dead
            EmailServiceProxy proxy = removeProxyIfDead(protocol);
            if (proxy == null) {
                Logging.warn(TAG, "Not in cache; now = " + mProxyCache);
            } else {
                Logging.warn(TAG, "Removed " + proxy + " from proxy cache; now = " + mProxyCache);
            }
            // Send "success" for all boxes in progress (get spinners to stop)
            for (long mid: mInProgressMailboxMap.keySet()) {
                Mailbox m = Mailbox.restoreMailboxWithId(this, mid);
                if (m == null || Account.getEmailProtocol(this, m.mAccountKey).equals(protocol)) {
                    mInProgressMailboxMap.remove(mid);
                    if (m != null) {
                        mEmailCallback.syncMailboxStatus(
                            mid, EmailServiceStatus.SUCCESS, null, false);
                        Logging.warn(TAG, protocol +
                            " disconnected; ending 'success' to mailbox " +
                                EmailLogging.mailboxName(this, mid, m.mAccountKey));
                    }
                }
            }
        }
        kick("Recover from disconnect");
    }

    private EmailServiceProxy getPersistentServiceForMailbox(Context context, Mailbox mailbox)
            throws RemoteException {
        synchronized (sControllerLock) {
            // For managed mailboxes, use the manager
            if (mailbox.isManaged()) {
                mailbox = Mailbox.restoreMailboxOfType(context, mailbox.mAccountKey,
                    Mailbox.TYPE_MANAGER);
            }
            if (mailbox != null) {
                ServiceEntry entry = mServiceMap.get(mailbox.mId);
                if (entry == null) {
                    Account account = Account.restoreAccountWithId(context, mailbox.mAccountKey);
                    String protocol = Account.getEmailProtocol(context, mailbox.mAccountKey);
                    entry =
                        new ServiceEntry(mailbox, protocol,
                            Mailbox.getSyncInterval(context, mailbox, account));
                    mServiceMap.put(mailbox.mId, entry);
                }
                return getPersistentServiceForProtocol(context, entry.mProtocol);
            }
            return null;
        }
    }

    private EmailServiceProxy getPersistentServiceForProtocol(Context context, String protocol)
            throws RemoteException {
        synchronized (sControllerLock) {
            EmailServiceProxy svc = getLiveProxy(protocol);
            if (svc == null) {
                svc = ServiceUtils.getService(context, mEmailCallback, protocol);
                svc.setProxyCallback(new EmailServiceProxyCallback(this, svc, protocol));
                int retries = 0;
                while (!svc.connect()) {
                    if (++retries == 1) {
                        ActivityManager manager =
                            (ActivityManager)getSystemService(Context.ACTIVITY_SERVICE);
                        int uid = android.os.Process.myUid();
                        for (RunningAppProcessInfo processInfo: manager.getRunningAppProcesses()) {
                            if (processInfo.uid == uid) {
                                String processName = processInfo.processName.toLowerCase();
                                Logging.warn(TAG, "Found process " + processName);
                                if (processName.endsWith(SYNC_PROCESS_SUFFIX)) {
                                    Logging.error(TAG, "Killing " + processName + "(" +
                                        processInfo.pid + ")");
                                    android.os.Process.killProcess(processInfo.pid);
                                }
                                break;
                            }
                        }
                    } else if (retries == 2) {
                        throw new RemoteException("Can't connect to " + protocol);
                    }
                }
                // Bind to this service (will throw if proxy has been killed)
                svc.bind();
                DeliveryMetrics.serviceUp(context, protocol);
                mProxyCache.put(protocol, svc);
                if (Logging.isProtocolLogging()) {
                    Logging.protocol(TAG, "Adding " + svc + " to the proxy cache: " + mProxyCache);
                }
            }
            return svc;
        }
    }

    private void stopServiceThreads(String protocol) {
        synchronized (sControllerLock) {
            for (ServiceEntry entry: mServiceMap.values()) {
                if (entry.mProtocol.equals(protocol)) {
                    try {
                        EmailServiceProxy svc = mProxyCache.get(protocol);
                        long mailboxId = entry.mMailboxId;
                        svc.stopSync(mailboxId);
                        //mPowerHandler.releaseWakeLock(mailboxId);
                        mServiceMap.remove(mailboxId);
                    } catch (RemoteException e) {
                        // It's all good...
                    }
                }
            }
        }
    }

    private void waitForConnectivity() throws InterruptedException {
        boolean waiting = false;
        while (!sStop) {
            NetworkInfo info = sConnectivityManager.getActiveNetworkInfo();
            if (info != null) {
                mNetworkInfo = info;
                // We're done if there's an active network
                if (waiting) {
                    // If we've been waiting, release any I/O error holds
                    releaseSyncHolds(this, EmailServiceStatus.CONNECTION_ERROR, null);
                }
                return;
            } else {
                // If this is our first time through the loop, shut down running service threads
                if (!waiting) {
                    waiting = true;
                    stopServiceThreads(null);
                }
                // Wait until a network is connected (or 10 mins), but let the device sleep
                // We'll set an alarm just in case we don't get notified (bugs happen)
                synchronized (sConnectivityLock) {
                    sPowerHandler.runAsleep(null, CONNECTIVITY_WAIT_TIME+5*SECONDS);
                    try {
                        Logging.log(TAG, "Connectivity lock...");
                        sConnectivityHold = true;
                        sConnectivityLock.wait(CONNECTIVITY_WAIT_TIME);
                        Logging.log(TAG, "Connectivity lock released...");
                    } finally {
                        sConnectivityHold = false;
                        sPowerHandler.runAwake(null);
                    }
                }
            }
        }
    }

    @Override
    public int onStartCommand(final Intent intent, int flags, int startId) {
        Logging.warn(TAG, "!!! onStartCommand, running = " +
                (sServiceThread != null));
        if (sServiceThread == null) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    synchronized (sControllerLock) {
                        /* PIM-17610
                         * Whenever this service is started, it tries to invoke the service thread.
                         * While attempting to start the thread, it sets the flag sStartingUp which
                         * denies anyone else to invoke the thread. Sometimes, due to a race condition,
                         * the call to service with the ADD_ACCOUNT intent action, which can invoke
                         * the service thread is denied the opportunity to do so. To avoid this, we
                         * remove the sStartingUp flag & make sure that the service thread is not already
                         * initialised before we attempt to start it (in a synchronized fashion). This
                         * will ensure that that the service thread is initialised only once.
                         */
                        if (sServiceThread ==  null) {
                            maybeStartControllerThread(intent);
                        }
                    }
                }}).start();
        } else if (intent != null) {
            // Request to sync a specific mailbox (used in UpsyncAlarmReceiver)
            Mailbox mailbox =
                intent.getParcelableExtra(UpsyncAlarmReceiver.UPSYNC_MAILBOX);
            ServiceController sc = sInstance;
            if (sc != null && mailbox != null) {
                startSync(sc, mailbox.mId, SYNC_UPSYNC);
            }
        }
        return Service.START_STICKY;
    }

    @Override
    public void onDestroy() {
        Logging.log(TAG, "!!! onDestroy");
    }

    public static void requestShutdown() {
        Logging.log(TAG, "!!! requestShutdown");
        new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (sControllerLock) {
                    // Stop the sync manager thread and return
                    if (sServiceThread != null) {
                        sStop = true;
                        sServiceThread.interrupt();
                    }
                }
            }}).start();
    }

    void maybeStartControllerThread(Intent intent) {
        // Start our thread...
        // See if there are any accounts (or we're creating one; otherwise, just go away
        if ((sServiceThread == null || !sServiceThread.isAlive())
                && !DemoModeHelper.getInstance(this).isDemo()) {
            boolean setupAccountAction =
                (intent != null &&
                    AccountSetupController.ADD_ACCOUNT_INTENT_ACTION.equals(intent.getAction()));
            if (setupAccountAction) {
                Logging.warn(TAG, "ADD_ACCOUNT_INTENT_ACTION found");
            }
            if (setupAccountAction || EmailContent.count(this, Account.CONTENT_URI) > 0) {
                Logging.log(TAG,
                    sServiceThread == null ? "Starting thread..." : "Restarting thread...");
                sInstance = this;
                Logging.log(TAG, "Service instance not null");
                sServiceThread = new Thread(this, TAG);
                sServiceThread.start();
                sPowerHandler = PowerHandler.getInstance(this, TAG);
                sConnectivityManager =
                    (ConnectivityManager)getSystemService(Context.CONNECTIVITY_SERVICE);
                NetworkInfo info = sConnectivityManager.getActiveNetworkInfo();
                if (info != null && info.isConnected()) {
                    DeliveryMetrics.networkUp(this, info.getTypeName());
                }
                return;
            }
        }

        stopSelf(); // If we didn't start the service thread, just stop the service
    }

    @Override
    public void run() {
        sStop = false;
        TempDirectory.setTempDirectory(this);

        // Synchronize here to prevent a shutdown from happening while we initialize our observers
        // and receivers
        synchronized (sControllerLock) {
            if (sServiceThread != null) {
                mResolver = getContentResolver();

                // Set up our observers; we need them to know when to start/stop various syncs based
                // on the insert/delete/update of mailboxes and accounts
                mMailboxObserver = new MailboxObserver(mHandler);
                mResolver.registerContentObserver(Mailbox.SERVICE_CONTROLLER_NOTIFIER_URI, true,
                    mMailboxObserver);

                registerAccountObservers();

                // We observe synced messages to trigger upsyncs at the appropriate time
                mSyncedMessageObserver = new SyncedMessageObserver(mHandler);
                mResolver.registerContentObserver(Message.NOTIFIER_URI, true,
                    mSyncedMessageObserver);

                // Set up receivers for connectivity and background data setting
                mConnectivityReceiver = new ConnectivityReceiver();
                registerReceiver(mConnectivityReceiver, new IntentFilter(
                    ConnectivityManager.CONNECTIVITY_ACTION));

                mBackgroundDataSettingReceiver = new ConnectivityReceiver();
                registerReceiver(mBackgroundDataSettingReceiver, new IntentFilter(
                    ConnectivityManager.ACTION_BACKGROUND_DATA_SETTING_CHANGED));
                // Save away the current background data setting; we'll keep track of it with the
                // receiver we just registered
                mBackgroundData = sConnectivityManager.getBackgroundDataSetting();
            }
        }

        try {
            // Loop indefinitely until we're shut down
            while (!sStop) {
                try {
                    sPowerHandler.runAwake(null);
                    waitForConnectivity();
                    mNextWaitReason = null;
                    long nextWait = checkMailboxes();
                    synchronized (sWaitLock) {
                        if (!sKicked) {
                            if (nextWait < 0) {
                                Logging.log(TAG, "Negative wait? Setting to 1s");
                                nextWait = MIN_WAIT_TIME;
                            }
                            if (nextWait > MIN_SLEEP_TIME) {
                                if (mNextWaitReason != null) {
                                    Logging.log(TAG, "Next awake ", nextWait / 1000, "s: ",
                                            mNextWaitReason);
                                }
                                sPowerHandler.runAsleep(null, nextWait + SLEEP_EXTRA_TIME);
                            }
                            sWaitLock.wait(nextWait);
                        }
                    }
                } catch (InterruptedException e) {
                    // Needs to be caught, but causes no problem
                    Logging.log(TAG, "Interrupted?");
                } finally {
                    synchronized (sWaitLock) {
                        sKicked = false;
                    }
                }
            }
            Logging.log(TAG, "Shutdown requested");
        } catch (RuntimeException e) {
            // Crash; this is a completely unexpected runtime error
            Logging.exception(TAG, e, "RuntimeException");
            throw e;
        } finally {
            boolean shouldRestart = !sStop; // we restart if its not an explicit shut down (crash)
            shutdown();
            // Try restarting?
            if (shouldRestart) {
                startService(new Intent(this, ServiceController.class));
            }
        }
    }

    private void shutdown() {
        synchronized (sControllerLock) {
            // If sServiceThread is null, we've already been shut down
            if (sServiceThread != null) {
                Logging.warn(TAG, "Shutting down...");

                // Stop our running syncs
                stopServiceThreads(null);

                // Stop receivers
                if (mConnectivityReceiver != null) {
                    safeUnregisterReceiver(mConnectivityReceiver);
                }
                if (mBackgroundDataSettingReceiver != null) {
                    safeUnregisterReceiver(mBackgroundDataSettingReceiver);
                }

                // Unregister observers
                ContentResolver resolver = getContentResolver();
                if (mSyncedMessageObserver != null) {
                    resolver.unregisterContentObserver(mSyncedMessageObserver);
                    mSyncedMessageObserver = null;
                }
                if (mMailboxObserver != null) {
                    resolver.unregisterContentObserver(mMailboxObserver);
                    mMailboxObserver = null;
                }
                unregisterAccountObservers();

                // Clear pending alarms and associated Intents
                sPowerHandler.clearAlarms();
                sPowerHandler.clearWakelocks();

                for (EmailServiceProxy proxy: mProxyCache.values()) {
                    sInstance.stopService(proxy.getIntent());
                }

                if (sStop) {
                    stopSelf();
                }

                sInstance = null;
                sServiceThread = null;
                sStop = false;

                Logging.log(TAG, "Goodbye");
            }
        }
    }

    /**
     * Unregister a BroadcastReceiver without crashing (We don't care it unregister several times)
     * @param receiver the BroadcastReceiver to unregister
     */
    private void safeUnregisterReceiver(BroadcastReceiver receiver){
        try {
            unregisterReceiver(receiver);
        } catch (RuntimeException e) {
            // Don't crash if we didn't register
            Logging.log(TAG, "The receiver " + receiver + " is no longer registered, " +
                    "hence we catch the exception and handle this safely.");
        }
    }
    /**
     * Release a mailbox from the service map and release its wake lock.
     * NOTE: This method MUST be called while holding sSyncLock!
     *
     * @param mailboxId the id of the mailbox to be released
     */
    public void releaseMailbox(long mailboxId) {
        mServiceMap.remove(mailboxId);
    }

    /**
     * Retrieve a running sync service for the passed-in mailbox id in a threadsafe manner
     *
     * @param mailboxId the id of the mailbox whose service is to be found
     * @return the running service (a subclass of AbstractSyncService) or null if none
     */
    public ServiceEntry getRunningService(long mailboxId) {
        synchronized(sControllerLock) {
            return mServiceMap.get(mailboxId);
        }
    }

    /**
     * Check whether an Outbox (referenced by a Cursor) has any messages that can be sent
     * @param outboxCursor the cursor to an Outbox
     * @return true if there is mail to be sent
     */
    private boolean hasSendableMessages(Cursor outboxCursor) {
        Cursor c = mResolver.query(Message.CONTENT_URI, Message.ID_COLUMN_PROJECTION,
            MessageUtils.MAILBOX_KEY_AND_NOT_SEND_FAILED,
            new String[] {Long.toString(outboxCursor.getLong(Mailbox.CONTENT_ID_COLUMN))},
            null);
        try {
            while (c.moveToNext()) {
                if (!Utility.hasUnloadedAttachments(this, c.getLong(Message.CONTENT_ID_COLUMN))) {
                    return true;
                }
            }
        } finally {
            if (c != null) {
                c.close();
            }
        }
        return false;
    }

    private boolean hasNewSendableMessages(long mailboxId) {
        Cursor c = mResolver.query(Message.CONTENT_URI, Message.ID_COLUMN_PROJECTION,
            MessageUtils.MAILBOX_KEY_AND_NEW, new String[] {Long.toString(mailboxId)}, null);
        try {
            return c != null && c.getCount() != 0;
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }

    private void markNewSendableMessagesSeen(long mailboxId) {
        // Mark new sendable messages seen so they won't be treated as NEW.
        ContentValues cv = new ContentValues();
        cv.put(Message.SERVER_ID, MessageUtils.SEND_SEEN);
        mResolver.update(Message.CONTENT_URI, cv, MessageUtils.MAILBOX_KEY_AND_NEW,
                new String[] {Long.toString(mailboxId)});
    }

    /**
     * Taken from ConnectivityManager using public constants
     */
    public static boolean isNetworkTypeMobile(int networkType) {
        switch (networkType) {
            case ConnectivityManager.TYPE_MOBILE:
            case ConnectivityManager.TYPE_MOBILE_MMS:
            case ConnectivityManager.TYPE_MOBILE_SUPL:
            case ConnectivityManager.TYPE_MOBILE_DUN:
            case ConnectivityManager.TYPE_MOBILE_HIPRI:
                return true;
            default:
                return false;
        }
    }

    /**
     * Determine whether the account is allowed to sync automatically, as opposed to manually, based
     * on whether the "require manual sync when roaming" policy is in force and applicable
     * @param account the account
     * @return whether or not the account can sync automatically
     */
    /*package*/ public boolean canAutoSync(Account account) {
        // Enforce manual sync only while roaming here
        long policyKey = account.mPolicyKey;
        // Quick exit from this check
        if ((policyKey != 0) && (mNetworkInfo != null) &&
                isNetworkTypeMobile(mNetworkInfo.getType())) {
            // We'll cache the Policy data here
            Policy policy = account.mPolicy;
            if (policy == null) {
                policy = Policy.restorePolicyWithId(this, policyKey);
                account.mPolicy = policy;
                if (!SecurityPolicy.getInstance(sInstance).isActive(policy)) {
                    Logging.log(TAG, "canAutoSync; policies not active, set hold flag");
                    return false;
                }
            }
            if (policy != null && policy.mRequireManualSyncWhenRoaming &&
                mNetworkInfo.isRoaming()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Convenience method to determine whether Email sync is enabled for a given account
     * @param account the Account in question
     * @return whether Email sync is enabled
     */
    private boolean canSyncEmail(android.accounts.Account account) {
        return ContentResolver.getSyncAutomatically(account, EmailContent.AUTHORITY);
    }

    private boolean canSyncEmail(Account account) {
        account.initGamsAccount(this);
        return canSyncEmail(account.mAmAccount);
    }
    /**
     * Determine whether a mailbox of a given type in a given account can be synced automatically
     * by SyncServiceManager.  This is an increasingly complex determination, taking into account
     * security policies and user settings (both within the Email application and in the Settings
     * application)
     *
     * @param account the Account that the mailbox is in
     * @param type the type of the Mailbox
     * @return whether or not to start a sync
     */
    private boolean isMailboxSyncable(Account account, int type) {

        // This 'if' statement performs checks to see whether or not a mailbox is a
        // candidate for syncing based on policies, user settings, & other restrictions
        if (type == Mailbox.TYPE_OUTBOX) {
            // Outbox is always syncable
            return true;
        } else if (type == Mailbox.TYPE_MANAGER) {
            // Always sync EAS mailbox unless master sync is off
            return ContentResolver.getMasterSyncAutomatically();
        } else if (type == Mailbox.TYPE_CONTACTS || type == Mailbox.TYPE_CALENDAR) {
            // Contacts/Calendar obey this setting from ContentResolver
            if (!ContentResolver.getMasterSyncAutomatically()) {
                return false;
            }
            // Get the right authority for the mailbox
            String authority;
            if (type == Mailbox.TYPE_CONTACTS) {
                authority = ContactsContract.AUTHORITY;
            } else {
                authority = CalendarContract.AUTHORITY;
            }
            account.initGamsAccount(this);
            // See if "sync automatically" is set; if not, punt
            if (!ContentResolver.getSyncAutomatically(account.mAmAccount, authority)) {
                return false;
            }
        // Never automatically sync trash
        } else if (type == Mailbox.TYPE_TRASH) {
            return false;
        // For non-outbox, non-account mail, we do three checks:
        // 1) are we restricted by policy (i.e. manual sync only),
        // 2) has the user checked the "Sync Email" box in Account Settings, and
        // 3) does the user have the master "background data" box checked in Settings
        } else if (!canAutoSync(account) || !canSyncEmail(account) || !mBackgroundData) {
            return false;
        }
        return true;
    }

    private void purgeDeletedMailboxes() {
        // First, see if any running mailboxes have been deleted
        synchronized (sControllerLock) {
            for (ServiceEntry entry: mServiceMap.values()) {
                long mailboxId = entry.mMailboxId;
                Mailbox m = Mailbox.restoreMailboxWithId(this, entry.mMailboxId);
                if (m == null) {
                    releaseMailbox(mailboxId);
                }
            }
        }
    }

    /**
     * Loop through all syncable mailboxes, starting syncs as required by 1) schedule, 2) push,
     * or 3) Outbox with sendable messages
     * @return time (in ms) til next required check of mailboxes
     */
    private long checkMailboxes () {
        // Check for deleted mailboxes
        purgeDeletedMailboxes();

        long nextWait = SERVICE_CONTROLLER_HEARTBEAT_TIME;
        long now = System.currentTimeMillis();

        Cursor c = getContentResolver().query(Mailbox.CONTENT_URI, Mailbox.CONTENT_PROJECTION,
            WHERE_OUTBOX_SENT_DRAFTS_PUSH_OR_SCHEDULED, null, null);
        try {
            while (c.moveToNext()) {
                long mailboxId = c.getLong(Mailbox.CONTENT_ID_COLUMN);
                ServiceEntry entry = getRunningService(mailboxId);

                // Check whether we're in a hold (temporary or permanent)
                if (entry != null && entry.mError != ServiceEntry.NO_ERROR) {
                    // Nothing we can do about fatal errors
                    if (EmailServiceStatus.isFatal(entry.mError)) {
                        // Show send error if outbox has new messages as they won't be sent.
                        if (Mailbox.TYPE_OUTBOX == c.getInt(Mailbox.CONTENT_TYPE_COLUMN) &&
                                hasNewSendableMessages(mailboxId)) {
                            markNewSendableMessagesSeen(mailboxId);
                            mEmailCallback.syncMailboxStatus(mailboxId, entry.mError, null, false);
                        }
                        continue;
                    }
                    long holdEndTime = entry.mHoldEndTime;
                    if (now < holdEndTime) {
                        // If release time is earlier than next wait time,
                        // move next wait time up to the release time
                        if (holdEndTime < now + nextWait) {
                            nextWait = holdEndTime - now;
                            mNextWaitReason = "Release hold";
                        }
                        continue;
                    } else {
                        // Remove the mailbox from the map; we'll start it fresh below
                        //releaseMailbox(mailboxId);
                        entry.mError = ServiceEntry.NO_ERROR;
                        startSyncFromCursor(c);
                        continue;
                    }
                }

                if (entry == null) {
                    // Get the cached account
                    Account account = Account.restoreAccountWithId(this,
                        c.getLong(Mailbox.CONTENT_ACCOUNT_KEY_COLUMN));
                    if (account == null) continue;

                    // We handle a few types of mailboxes specially
                    int mailboxType = c.getInt(Mailbox.CONTENT_TYPE_COLUMN);
                    if (!isMailboxSyncable(account, mailboxType)) {
                        continue;
                    }

                    int syncInterval = c.getInt(Mailbox.CONTENT_SYNC_INTERVAL_COLUMN);
                    // We don't handle managed mailboxes; non-managed inbox uses account's interval
                    if (mailboxType == Mailbox.TYPE_INBOX &&
                            syncInterval == Mailbox.CHECK_INTERVAL_MANAGED) {
                        continue;
                    } else {
                        syncInterval =
                            Mailbox.getSyncInterval(this, syncInterval, mailboxType, account);
                    }

                    handleDraftUpSync(account);
                    if (syncInterval == Mailbox.CHECK_INTERVAL_PUSH) {
                        startSyncFromCursor(c);
                    } else if (mailboxType == Mailbox.TYPE_OUTBOX) {
                        if (hasSendableMessages(c)) {
                            startSyncFromCursor(c);
                        }
                    } else if ((syncInterval & Mailbox.CHECK_INTERVAL_ONCE_BIT) != 0) {
                        // Handle this case
                    } else if (syncInterval >= 5 && syncInterval <= ONE_DAY_MINUTES) {
                        long lastSync = c.getLong(Mailbox.CONTENT_SYNC_TIME_COLUMN);
                        long sinceLastSync = now - lastSync;
                        long toNextSync = (long) syncInterval*MINUTES - sinceLastSync;
                        String name = EmailLogging.mailboxName(this, mailboxId, account.mId);
                        if (toNextSync <= 0) {
                            startSyncFromCursor(c);
                        } else if (toNextSync < nextWait) {
                            nextWait = toNextSync;
                            Logging.log(TAG, "Next sync for ", name, " in ", nextWait/1000, "s");
                            mNextWaitReason = "Scheduled sync, " + name;
                        } else {
                            Logging.log(TAG, "Next sync for ", name, " in ", toNextSync/1000, "s");
                        }
                    }
                }
            }
        } finally {
            c.close();
        }
        return nextWait;
    }

    private void handleDraftUpSync(final Account account) {
        if (!AccountUtils.isDraftUpsyncSupported(account)) {
            return;
        }
        long draftId = Mailbox.findMailboxOfType(this, account.mId, Mailbox.TYPE_DRAFTS);
        String draftKey = String.valueOf(draftId);
        Cursor cursor = getContentResolver().query(EmailContent.Message.CONTENT_URI,
                Message.LIST_PROJECTION, WHERE_MAILBOX_KEY_AND_DRAFT_MESSAGE,
                new String[]{draftKey}, null);
        if (cursor == null) {
            // Just return. Don't handle Up-sync.
            return;
        }
        try {
            if (cursor.getCount() > 0) {
                final ArrayList<Long> messageIds = new ArrayList();
                while (cursor.moveToNext()) {
                    final long messageId = cursor.getLong(Message.LIST_ID_COLUMN);
                    messageIds.add(messageId);
                }
                if (messageIds.size() > 0) {
                    syncLocalDraft(this, messageIds, account);
                }
            }
        } finally {
            cursor.close();
        }
    }

    /**
     * Return a boolean indicating whether the mailbox can be synced
     * @param context the context
     * @param account the account
     * @param m the mailbox
     * @return whether or not the mailbox can be synced
     */
    public static boolean isSyncable(Context context, Account account, Mailbox m) {
        // Filter out the known mailbox types first.
        if (account == null || m == null || m.mType == Mailbox.TYPE_OUTBOX ||
            m.mType == Mailbox.TYPE_DRAFTS || m.mType == Mailbox.TYPE_SEARCH ||
            m.mType >= Mailbox.TYPE_NOT_SYNCABLE) {
            return false;
        }
        // For POP3 accounts, only INBOX is syncable.
        return (m.mType == Mailbox.TYPE_INBOX ||
                !ServiceInfo.SERVICE_GENERIC_POP3.equals(account.mServiceType));
    }

    /**
     * Wake up SyncServiceManager to check for mailboxes needing service
     */
    public static void kick(String reason) {
        synchronized (sWaitLock) {
            //if (reason != null) {
            //    log("Kick: " + reason);
            //}
            sKicked = true;
            sWaitLock.notify();
        }
        if (sConnectivityLock != null) {
            synchronized (sConnectivityLock) {
                sConnectivityLock.notify();
            }
        }
    }

    private void cbLog(String text, long mailboxId) {
        if (Logging.isLogging()) {
            String name = EmailLogging.mailboxName(this, mailboxId, -1);
            Logging.log(TAG, text + ": " + name);
        }
    }

    public void cbLog(String method, long mailboxId, long accountId, int status, String info) {
        if (Logging.isLogging()) {
            String name = EmailLogging.mailboxName(this, mailboxId, accountId);
            Logging.log(TAG, method, ": ", name, ", ", EmailServiceStatus.toString(status));
        }
    }

    public void cbLog(String method, long messageId, int status) {
        if (Logging.isLogging()) {
            Logging.log(TAG, method, ": msg #", messageId, ", ",
                EmailServiceStatus.toString(status));
        }
    }

    public static void addCallback(EmailServiceCallback cb) {
        sCallbackList.register(cb);
    }

    public static void removeCallback(EmailServiceCallback cb) {
        sCallbackList.unregister(cb);
    }

    private class ServiceCallback extends EmailServiceCallback {

        private ServiceCallback(RemoteCallbackList<IEmailServiceCallback> cbl) {
            super(cbl);
        }

        @Override
        public void syncMailboxStatus(final long mailboxId, final int status,
            final String info, final boolean background) {
            cbLog(">> syncMailboxStatus", mailboxId, -1, status, info);

            super.syncMailboxStatus(mailboxId, status, info, background);

            synchronized(sControllerLock) {
                // Keep track of which syncs are in progress
                if (status == EmailServiceStatus.IN_PROGRESS) {
                    mInProgressMailboxMap.put(mailboxId, true);
                } else {
                    mInProgressMailboxMap.remove(mailboxId);
                }

                ConcurrentHashMap<Long, ServiceEntry> map = mServiceMap;
                ServiceEntry entry = map.get(mailboxId);
                if (entry == null) {
                    if (!mManagedMailboxMap.containsKey(mailboxId)) {
                        Mailbox m = Mailbox.restoreMailboxWithId(ServiceController.this, mailboxId);
                        if (m != null) {
                            if (m.isManaged()) {
                                long managerId = Mailbox.findMailboxOfType(ServiceController.this,
                                    m.mAccountKey, Mailbox.TYPE_MANAGER);
                                if (managerId != Mailbox.NO_MAILBOX) {
                                    mManagedMailboxMap.put(mailboxId, managerId);
                                } else {
                                    Logging.error("Unmanaged mailbox has no manager: " + mailboxId);
                                }
                            }
                        }
                    }
                    cbLog("Not managed by ServiceController", mailboxId);
                    return;
                }
                switch(status) {
                    case EmailServiceStatus.TERMINATED:
                        kick("Sync ended");
                        // Only remove from map if we're not in an error state
                        if (entry.mError == ServiceEntry.NO_ERROR) {
                            cbLog("Terminated; removed from serviceMap", mailboxId);
                            map.remove(mailboxId);
                        } else {
                            cbLog("Terminated; keeping in serviceMap with error " + entry.mError,
                                mailboxId);
                        }
                        break;
                    case EmailServiceStatus.SUCCESS:
                    case EmailServiceStatus.SEND_MAIL_FAILURE:
                        // Note that SEND_MAIL_FAILURE will be picked up by other callback handlers
                        // so that the banner will still appear; we don't want this holding up
                        // other syncs
                        NotificationController.getInstance(ServiceController.this)
                            .cancelLoginFailedNotification(entry.mAccountId);
                        // Reset hold delay & error code
                        entry.mHoldDelay = HOLD_DELAY_MINIMUM;
                        entry.resetError();
                        break;
                    case EmailServiceStatus.CONNECTION_ERROR:
                        entry.backoff();
                        cbLog("Will retry in " + entry.mHoldDelay, mailboxId);
                        kick("Schedule retry");
                        break;
                    case EmailServiceStatus.IN_PROGRESS:
                        cbLog("Sync started", mailboxId);
                        break;
                    case EmailServiceStatus.LOGIN_FAILED:
                        cbLog("Login failed", mailboxId);
                        entry.mError = status;
                        NotificationController.getInstance(ServiceController.this)
                            .showLoginFailedNotification(entry.mAccountId);
                        break;
                    default:
                        cbLog("Error set to " + EmailServiceStatus.toString(status), mailboxId);
                        entry.mError = status;
                }
            }
        }
    };

    /**
     * Sent by services indicating that their thread is finished; action depends on the exitStatus
     * of the service.
     *
     * @param svc the service that is finished
     */

    /**
     * Given the status string from a Mailbox, return the type code for the last sync
     * @param status the syncStatus column of a Mailbox
     * @return
     */
    static public int getStatusType(String status) {
        if (status == null) {
            return -1;
        } else {
            return status.charAt(STATUS_TYPE_CHAR) - '0';
        }
    }

    /**
     * Given the status string from a Mailbox, return the change count for the last sync
     * The change count is the number of adds + deletes + changes in the last sync
     * @param status the syncStatus column of a Mailbox
     * @return
     */
    static public int getStatusChangeCount(String status) {
        try {
            String s = status.substring(STATUS_CHANGE_COUNT_OFFSET);
            return Integer.parseInt(s);
        } catch (RuntimeException e) {
            return -1;
        }
    }

    @Override
    public IBinder onBind(Intent arg0) {
        return null;
    }

    @Override
    public void dump(FileDescriptor fd, PrintWriter pw, String[] args) {
        if (sPowerHandler != null) {
            sPowerHandler.dump(fd,  pw,  args);
        }
    }

    private static IEmailService getAdHocServiceForMessage(Context context, long messageId) {
        Message message = Message.restoreMessageWithId(context, messageId);
        if (message == null) {
            return null;
        }
        return ServiceUtils.getServiceForAccount(context, null, message.mAccountKey);
    }

    private static IEmailService getAdHocServiceForAccount(Context context, long accountId) {
        return ServiceUtils.getServiceForAccount(context, null, accountId);
    }

    /**
     * The UI controls services through these methods
     */

    public static void startSync(final Context context, final long mailboxId, final int reason) {
        // Service connections could take time, so we'll run this async
        new Thread(new Runnable() {
            @Override
            public void run() {
                if (Logging.isLogging()) {
                    Logging.info(TAG, "Start sync (", getSyncReason(reason), "): " +
                            EmailLogging.mailboxName(context, mailboxId, Account.NO_ACCOUNT));
                }
                Mailbox mailbox = Mailbox.restoreMailboxWithId(context, mailboxId);
                if (mailbox == null) return;
                ServiceController sc = sInstance;
                if (sc == null) {
                    // If upsync, we don't care (next sync will catch it)
                    // Otherwise, log this but don't crash
                    if (reason != ServiceController.SYNC_UPSYNC) {
                        Logging.warn(TAG, "Sync can't be started; ServiceController not running");
                    }
                } else {
                    sc.startSync(context, mailbox, reason);
                }
            }}).start();
    }

    public static Bundle validate(Context context, Account account, boolean commitOnSuccess)
            throws RemoteException {
        ServiceController sc = sInstance;
        if (sc == null) {
            Logging.log(TAG, "Service instance is null");
            throw new IllegalStateException("ServiceController not running");
        }
        ServiceInfo info = ServiceUtils.getServiceInfo(context, account.mServiceType);
        if (info == null) {
            throw new IllegalArgumentException("Account doesn't have a valid serviceType");
        }
        EmailServiceProxy service = sc.getPersistentServiceForProtocol(context, info.emailProtocol);
        return service.validate(account, commitOnSuccess);
    }

    public static void loadFullMessage(final Context context, final long messageId) {
        Logging.log(TAG, "UI requests load full message ", messageId);
        IEmailService service = getAdHocServiceForMessage(context, messageId);
        if (service != null) {
            try {
                service.loadFullMessage(messageId);
            } catch (RemoteException e) {
                Logging.error(TAG, "RemoteException in loadFullMessage");
            }
        }
    }

    public static void hostChanged(Context context, long accountId) throws RemoteException {
        Account acct = Account.restoreAccountWithId(context, accountId);
        if (acct == null) return;
        ServiceController sc = sInstance;
        if (sc == null) {
            // No harm, syncs won't be running if ServiceController hasn't started up
            return;
        }
        sc.releaseSyncHolds(context, EmailServiceStatus.LOGIN_FAILED, acct);
    }

    // For UI use; request that an attachment be downloaded
    public static void requestLoadAttachment(Context context, long attachmentId) {
        Attachment att = Attachment.restoreAttachmentWithId(context, attachmentId);
        if (att == null) return;

        if (Utility.attachmentExists(context, att)) {
            return;
        }

        // Flag the attachment as needing download at the user's request
        ContentValues cv = new ContentValues();
        cv.put(Attachment.FLAGS, att.mFlags | Attachment.FLAG_DOWNLOAD_USER_REQUEST);
        att.update(context, cv);

        Logging.log(TAG, "UI requests load of attachment ", attachmentId);
    }

    // For AttachmentDownloadService use
    public static void loadAttachment(Context context, long attachmentId, long attachmentAccountId,
            boolean background) {
        IEmailService service = getAdHocServiceForAccount(context, attachmentAccountId);
        if (service != null) {
            try {
                service.loadAttachment(attachmentId, background);
            } catch (RemoteException e) {
                Logging.error(TAG, "RemoteException in loadAttachment");
            }
        }
    }

    public void cancelLoadAttachment(final long attachmentId) {
        Attachment att = Attachment.restoreAttachmentWithId(this, attachmentId);
        if (att == null) {
            return;
        }
        // Remove the user request flag from the attachemnt info
        ContentValues cv = new ContentValues();
        cv.put(Attachment.FLAGS, att.mFlags & ~Attachment.FLAG_DOWNLOAD_USER_REQUEST);
        att.update(this, cv);
    }

    /**
     * Respond to a meeting invitation.
     *
     * @param messageId the id of the invitation being responded to
     * @param response the code representing the response to the invitation
     */
    public static void sendMeetingResponse(Context context, final long messageId,
            final int response) {
        IEmailService service = getAdHocServiceForMessage(context, messageId);
        if (service != null) {
            try {
                service.sendMeetingResponse(messageId, response);
            } catch (RemoteException e) {
                Logging.error(TAG, "RemoteException in sendMeetingResponse");
            }
        }
    }

    /**
     * Request a remote update of mailboxes for an account.
     */
    public static void updateMailboxList(Context context, final long accountId) {
        IEmailService service = getAdHocServiceForAccount(context, accountId);
        if (service != null) {
            try {
                service.updateFolderList(accountId);
            } catch (RemoteException e) {
                Logging.error(TAG, "RemoteException in updateMailboxList");
            }
        }
    }

    private static final LongSparseArray<SearchParams> sSearchParamsMap =
            new LongSparseArray<>();

    public static void searchMore(Context context, long accountId) throws MessagingException {
        SearchParams params = sSearchParamsMap.get(accountId);
        if (params == null) return;
        params.mOffset += params.mLimit;
        searchMessages(context, accountId, params);
    }

    /**
     * Search for messages on the (IMAP) server; do not call this on the UI thread!
     * @param accountId the id of the account to be searched
     * @param searchParams the parameters for this search
     * @throws MessagingException
     */
    public static int searchMessages(final Context context, final long accountId,
        final SearchParams searchParams) throws MessagingException {
        // Find/create our search mailbox
        Mailbox searchMailbox = Mailbox.getSearchMailbox(context, accountId);
        final long searchMailboxId = searchMailbox.mId;
        // Save this away (per account)
        sSearchParamsMap.put(accountId, searchParams);

        if (searchParams.mOffset == 0) {
            // Delete existing contents of search mailbox
            ContentResolver resolver = context.getContentResolver();
            resolver.delete(Message.CONTENT_URI, Message.MAILBOX_KEY + "=" + searchMailboxId,
                    null);
            ContentValues cv = new ContentValues();
            // For now, use the actual query as the name of the mailbox
            cv.put(Mailbox.DISPLAY_NAME, searchParams.mQuery);
            resolver.update(ContentUris.withAppendedId(Mailbox.CONTENT_URI, searchMailboxId),
                    cv, null, null);
        }

        IEmailService service = getAdHocServiceForAccount(context, accountId);
        if (service != null) {
            // Service implementation
            try {
                return service.searchMessages(accountId, searchParams, searchMailboxId);
            } catch (RemoteException e) {
                Logging.exception("searchMessages", e, "RemoteException");
                return 0;
            }
        } else {
            throw new IllegalStateException("No service found");
        }
    }

    /**
     * Increase the load count for a given mailbox, and trigger a refresh.  Applies only to
     * IMAP and POP mailboxes, with the exception of the EAS search mailbox.
     *
     * @param mailboxId the mailbox
     */
    public static void loadMoreMessages(final Context context, final long mailboxId) {
        // FIXME: This call was runAsyncParallel, to reduce rejectedExecutionException
        // the call is made serial. Refer: PIM-17503
        EmailAsyncTask.runAsyncSerial(new Runnable() {
            @Override
            public void run() {
                Mailbox mailbox = Mailbox.restoreMailboxWithId(context, mailboxId);
                if (mailbox == null) {
                    return;
                }
                if (mailbox.mType == Mailbox.TYPE_SEARCH) {
                    try {
                        searchMore(context, mailbox.mAccountKey);
                    } catch (MessagingException e) {
                        // Nothing to be done
                    }
                    return;
                }
                AccountSettingsUtils.increaseSyncWindow(context, mailbox);
                ServiceController.startSync(context, mailboxId, ServiceController.SYNC_UI_REFRESH);
            }
        });
    }

    private static final String UNSET_WARN_FLAG_SELECT = Message.MAILBOX_KEY + "=?";

    /**
     * Unset attachment warning flags for all messages in outbox
     */
    public static void unsetAttachmentSizeWarnings(final Context context, final long accountId) {
        // FIXME: This call was runAsyncParallel, to reduce rejectedExecutionException
        // the call is made serial. Refer: PIM-17503
        EmailAsyncTask.runAsyncSerial(new Runnable() {
            @Override
            public void run() {
                final long outboxId = Mailbox.findMailboxOfType(context, accountId,
                        Mailbox.TYPE_OUTBOX);
                if (outboxId == Mailbox.NO_MAILBOX) {
                    return;
                }
                ContentResolver resolver = context.getContentResolver();
                resolver.update(EmailContent.Message.UNSET_WARN_FLAG_URI, null,
                        UNSET_WARN_FLAG_SELECT, new String[] { Long.toString(outboxId) });
            }
        });
    }

    public static boolean isPushInProgress(Context context, long mailboxId) {
        ServiceController controller = sInstance;
        if (controller == null) {
            // By definition, it's not... :-)
            return false;
        }
        ConcurrentHashMap<Long, Long> map = controller.mManagedMailboxMap;
        if (map.containsKey(mailboxId)) {
            mailboxId = map.get(mailboxId);
        }
        // Ok, this is the mailboxId of the controlling mailbox
        ServiceEntry entry = controller.mServiceMap.get(mailboxId);
        return (entry != null) && (entry.mError == ServiceEntry.NO_ERROR) &&
            (entry.mSyncInterval == Mailbox.CHECK_INTERVAL_PUSH);
    }

    public static String getSyncReason(int reason) {
        if (reason >= 0 && reason < SYNC_REASONS.length) {
            return SYNC_REASONS[reason];
        }
        return "None";
    }

    public static void breakPing(Context context, long accountId) {
        Mailbox m = Mailbox.getMailboxForType(context, accountId, Mailbox.TYPE_INBOX);
        startSync(context, m.mId, SYNC_REASON_ANY);
    }

    private EmailServiceProxy getLiveProxy(String protocol) {
        synchronized(sConnectivityLock) {
            EmailServiceProxy proxy = mProxyCache.get(protocol);
            // treat dead proxies in our cache as if they weren't there
            return (proxy != null && proxy.isDead()) ? null : proxy;
        }
    }

    private EmailServiceProxy removeProxyIfDead(String protocol) {
        synchronized(sConnectivityLock) {
            EmailServiceProxy proxy = mProxyCache.get(protocol);
            return (proxy != null && proxy.isDead()) ? mProxyCache.remove(protocol) : null;
        }
    }
    /**
     * Sends the out of office setting change to the server.
     * @param context
     * @param accountId id of account for which we have to set OOF
     * @param info the OOF settings to be uploaded
     * @return
     */
    public static boolean setOutOfOfficeState(final Context context, final long accountId,
        final OutOfOffice info) {
        IEmailService service = getAdHocServiceForAccount(context, accountId);
        if (service != null) {
            try {
                return service.setOutOfOfficeState(accountId, info);
            } catch (RemoteException e) {
                Logging.exception(TAG, e, "RemoteException");
                return false;
            }
        } else {
            throw new IllegalStateException("No service found");
        }
    }

    /**
     * Gets the current OOF status from the server
     * @param context
     * @param accountId id for whom we want to retreive OOF settings.
     * @return OutOfOfficeInfo object containing current state for OOF for account
     */
    public static boolean getOutOfOfficeState(final Context context, final long accountId) {
        IEmailService service = getAdHocServiceForAccount(context, accountId);
        if (service != null) {
            try {
                return service.getOutOfOfficeState(accountId);
            } catch (RemoteException e) {
                Logging.exception(TAG, e, "RemoteException");
                return false;
            }
        } else {
            throw new IllegalStateException("No service found");
        }
    }

    /**
     * Sends local Draft to EAS Server
     *
     * @param context
     * @param messageIds the ids of the Draft messages to be up synced
     */
    public static void syncLocalDraft(final Context context, final List<Long> messageIds,
                                      final Account account) {
        if (AccountUtils.isDraftUpsyncSupported(account)) {
            final IEmailService service = getAdHocServiceForMessage(context, messageIds.get(0));
            if (service != null) {
                try {
                    service.syncLocalDraft(Longs.toArray(messageIds));
                } catch (RemoteException e) {
                    Logging.error(TAG, "RemoteException in syncLocalDraft");
                }
            }
        }
    }
}