package concurrency;

import android.content.Context;
import android.content.SharedPreferences;
import android.os.Environment;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.support.annotation.VisibleForTesting;
import android.text.TextUtils;
import com.amazon.gallery.foundation.metrics.Profiler;
import com.amazon.gallery.foundation.utils.log.GLogger;
import com.amazon.gallery.framework.kindle.auth.AuthenticationManager;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.Completable;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static com.amazon.gallery.framework.preferences.NonMemberSettingsMigrationMetricsHelper.MetricsEvent;
import static com.amazon.gallery.framework.preferences.PreferencesUtils.GET_ACCOUNT_ID_RETRY_INTERVAL;


/**
 * Facilitates non-member settings migrations to member mode, i.e. when
 * there is a logged in user, by synchronizing settings between external
 * storage directory and SharedPreferences. The former stores non-member settings
 * to be migrated on top of SharedPreferences once there is a logged in user.
 * The settings stored in SharedPreferences are used as the source of truth
 * for as long as the device is in a non-member mode. In short, this class
 * maintains the following two way synchronization:
 *     When in non-member mode:
 *         continuous: SharedPreferences -> External Storage Directory
 *     When in member mode:
 *         one-time: External Storage Directory -> SharedPreferences
 *
 * All settings data that is stored in external storage directory is serialized
 * as JSON. The corresponding backing file is shared across all settings, which
 * is why it is imperative that each has a unique key.
 */
public class NonMemberPreferencesMigrationHelper {

    private static final String TAG = NonMemberPreferencesMigrationHelper.class.getSimpleName();
    private static final String ABS_PATH_FORMAT = "%s/com.amazon.photos.NonMemberPreferencesMigrationHelper.json";

    @NonNull private final ObjectMapper mapper;
    @NonNull private final File file;
    @NonNull private final TypeReference<ConcurrentHashMap<String, Object>> typeRef;
    @NonNull private final CountDownLatch loadedFromDisk;
    @NonNull private ConcurrentHashMap<String, Object> map;
    @NonNull NonMemberSettingsMigrationMetricsHelper metricsHelper;

    private Disposable commitToDiskDisposable;
    private String accountIdSettingsSynchronizationDoneFor;

    public NonMemberPreferencesMigrationHelper(@Nullable final Profiler profiler) {
        mapper = new ObjectMapper();
        typeRef = new TypeReference<ConcurrentHashMap<String, Object>>() { };
        file = new File(String.format(ABS_PATH_FORMAT,
                Environment.getExternalStorageDirectory().getAbsolutePath()));
        loadedFromDisk = new CountDownLatch(1);
        map = new ConcurrentHashMap<>();
        metricsHelper = new NonMemberSettingsMigrationMetricsHelper(profiler);

        loadFromDisk();
    }

    @VisibleForTesting
    public NonMemberPreferencesMigrationHelper() {
        this(null);
    }

    /**
     * Retrieve a String value from the persistent preferences.
     * @param key The name of the preference to retrieve.
     * @param defaultValue Value to return if this preference does not exist.
     * @return Returns the preference value if it exists, or defaultValue. Throws
     * ClassCastException if there is a preference with this name that is not
     * a String.
     *
     * @throws ClassCastException If setting value's type is not String.
     */
    @Nullable
    public String getString(@NonNull final String key, @Nullable final String defaultValue) {
        awaitLoadFromDisk();
        return map.containsKey(key) ? (String)map.get(key) : defaultValue;
    }

    /**
     * Set a String value in the persistent preferences store.
     * @param key The name of the preference to modify.
     * @param value The new value for the preference.
     */
    public void putString(@NonNull final String key, @NonNull final String value) {
        awaitLoadFromDisk();
        map.put(key, value);
        commitToDisk();
    }

    /**
     * Retrieve a boolean value from the persistent preferences.
     *
     * @param key The name of the preference to retrieve.
     * @param defaultValue Value to return if this preference does not exist.
     *
     * @return Returns the preference value if it exists, or defaultValue. Throws
     * ClassCastException if there is a preference with this name that is not
     * a boolean.
     *
     * @throws ClassCastException If setting value's type is not Boolean.
     */
    @Nullable
    public Boolean getBoolean(@NonNull final String key, @Nullable final Boolean defaultValue) {
        awaitLoadFromDisk();
        return map.containsKey(key) ? (Boolean) map.get(key) : defaultValue;
    }

    /**
     * Set a Boolean value in the persistent preferences store.
     *
     * @param key The name of the preference to modify.
     * @param value The new value for the preference.
     */
    public void putBoolean(@NonNull final String key, @NonNull final Boolean value) {
        awaitLoadFromDisk();
        map.put(key, value);
        commitToDisk();
    }

    /**
     * Remove key/value pair for a given key.
     * @param key Key to remove key/value pair for.
     * @return Value that was removed, if any.
     */
    @Nullable
    public Object remove(@NonNull final String key) {
        awaitLoadFromDisk();

        Object removedObject = map.remove(key);
        if (removedObject != null) {
            commitToDisk();
        }

        return removedObject;
    }

    /**
     * Checks if a key/value pair exists for given key.
     * @param key Key to check if key/value pair exists for.
     * @return True if key/value pair exists, false otherwise.
     */
    public boolean contains(@NonNull final String key) {
        awaitLoadFromDisk();
        return map.containsKey(key);
    }

    /**
     * Synchronizes settings data in one of two directions, depending on current device mode.
     *
     * Non-member mode (i.e. there is no logged in user):
     *  In this mode settings data is copied from SharedPreferences to a backup location.
     *  The former is treated as a single source of truth and the latter should mirror
     *  the former when synchronization is finished.
     *
     * Member mode (i.e. there's a logged in user):
     *  In this mode settings data is copied from backup location to SharedPreferences. This
     *  represents a one-time migration of settings upon a user logging in to the device.
     *
     * The settings to be synchronized are defined by the keysForStringValues and keysForBooleanValues
     * parameters, which contain a list of keys.
     *
     * @param keysForStringValues Keys for settings whose values are Strings.
     * @param keysForBooleanValues Keys for settings whose values are Boolean.
     * @param sharedPreferences An instance of SharedPreferences to synchronize settings with. Whether
     *                          data is read from or written to it depends on whether there is a logged
     *                          in user.
     * @param overridesAndSharedPrefsSyncComplete A synchronization primitive used to signal when
     *                                            synchronization is complete.
     * @param context Application context.
     * @param authenticationManager MAP authentication manager.
     */
    void scheduleNonMemberSettingsSynchronization(@NonNull final List<String> keysForStringValues,
                                                  @NonNull final List<String> keysForBooleanValues,
                                                  @NonNull final SharedPreferences sharedPreferences,
                                                  @NonNull final CountDownLatch overridesAndSharedPrefsSyncComplete,
                                                  @NonNull final Context context,
                                                  @NonNull final AuthenticationManager authenticationManager) {
        GLogger.i(TAG, "Scheduling non-member overrides and SharedPreferences synchronization.");
        Completable.fromAction(() -> {
            try {
                GLogger.i(TAG, "Starting non-member overrides and SharedPreferences synchronization.");
                String accountId = authenticationManager.getAccountId(GET_ACCOUNT_ID_RETRY_INTERVAL);

                // The following synchronization is to be performed only once for a given
                // direction for the lifetime of this object. To detect cases where this is
                // scheduled twice in the same direction, e.g. once as part of the constructor
                // and once in response to a broadcast, make a note of the associated account ID
                // and return immediately if account ID has not changed.
                if (accountId.equals(accountIdSettingsSynchronizationDoneFor)) {
                    return;
                }
                accountIdSettingsSynchronizationDoneFor = accountId;

                if (MapAccountPreferences.isNonMemberMode(context, accountId)) {
                    for (String key : keysForStringValues) {
                        // Ensure that preference overrides reflect latest values in SharedPreferences.
                        if (synchronizeStringSettingWithSharedPrefs(sharedPreferences, key)) {
                            GLogger.i(TAG, "Synchronized non-member setting value for %s.", key);
                            metricsHelper.reportMetric(MetricsEvent.SettingStashedForMigration);
                        }
                    }

                    for (String key : keysForBooleanValues) {
                        // Ensure that preference overrides reflect latest values in SharedPreferences.
                        if (synchronizeBooleanSettingWithSharedPrefs(sharedPreferences, key)) {
                            GLogger.i(TAG, "Synchronized non-member setting value for %s.", key);
                            metricsHelper.reportMetric(MetricsEvent.SettingStashedForMigration);
                        }
                    }
                } else {
                    for (String key : keysForStringValues) {
                        // Transfer preferences overrides to SharedPreferences to reflect changes done
                        // during non-member mode, if any.
                        if (migrateStringSettingToSharedPrefs(sharedPreferences, key)) {
                            GLogger.i(TAG, "Migrated non-member setting value for %s.", key);
                            metricsHelper.reportMetric(MetricsEvent.SettingMigrated);
                        }
                    }
                    for (String key : keysForBooleanValues) {
                        // Transfer preferences overrides to SharedPreferences to reflect changes done
                        // during non-member mode, if any.
                        if (migrateBooleanSettingToSharedPrefs(sharedPreferences, key)) {
                            GLogger.i(TAG, "Migrated non-member setting value for %s.", key);
                            metricsHelper.reportMetric(MetricsEvent.SettingMigrated);
                        }
                    }
                }
            } finally {
                overridesAndSharedPrefsSyncComplete.countDown();
            }
        }).subscribeOn(Schedulers.io())
                .subscribe(
                        () -> {
                            GLogger.i(TAG, "Successfully migrated/synchronized non-member preferences.");
                            metricsHelper.reportMetric(MetricsEvent.SettingsSyncSuccess);
                        },
                        (throwable) -> {
                            GLogger.ex(TAG, "Failed migrating/synchronizing non-member preferences.", throwable);
                            metricsHelper.reportMetric(MetricsEvent.SettingsSyncFailure);
                        }
                );
    }

    /**
     * Helper method to migrate a String preference value from external storage directory
     * to SharedPreferences.
     * @param sharedPreferences Instance of SharedPreferences to migrate setting to.
     * @param key The name of the setting to migrate.
     * @return True if the migration took place, false otherwise.
     */
    private boolean migrateStringSettingToSharedPrefs(@NonNull final SharedPreferences sharedPreferences,
                                                      @NonNull final String key) {
        if (contains(key)) {
            String valueToOverride = getString(key, null);
            sharedPreferences.edit().putString(key, valueToOverride).apply();
            remove(key);
            return true;
        }

        return false;
    }

    /**
     * Helper method to migrate a Boolean preference value from external storage directory
     * to SharedPreferences.
     * @param sharedPreferences Instance of SharedPreferences to migrate setting to.
     * @param key The name of the setting to migrate.
     * @return True if the migration took place, false otherwise.
     */
    private boolean migrateBooleanSettingToSharedPrefs(@NonNull final SharedPreferences sharedPreferences,
                                                       @NonNull final String key) {
        if (contains(key)) {
            Boolean valueToOverride = getBoolean(key, null);
            sharedPreferences.edit().putBoolean(key, valueToOverride).apply();
            remove(key);
            return true;
        }

        return false;
    }

    /**
     * Helper method to synchronize a String setting from SharedPreferences to
     * external storage directory. Deletes destination value, if source is not present.
     * @param sharedPreferences Instance of SharedPreferences to synchronize setting with.
     * @param key The name of the preference to synchronize.
     * @return True, if synchronization took place, false otherwise.
     */
    private boolean synchronizeStringSettingWithSharedPrefs(@NonNull final SharedPreferences sharedPreferences,
                                                            @NonNull final String key) {
        String valueToSynchronize = sharedPreferences.getString(key, null);
        if (!TextUtils.isEmpty(valueToSynchronize)) {
            putString(key, valueToSynchronize);
            return true;
        }

        return remove(key) != null;
    }

    /**
     * Helper method to synchronize a Boolean setting from SharedPreferences to
     * external storage directory. Deletes destination value, if source is not present.
     * @param sharedPreferences Instance of SharedPreferences to synchronize setting with.
     * @param key The name of the preference to synchronize.
     * @return True, if synchronization took place, false otherwise.
     */
    private boolean synchronizeBooleanSettingWithSharedPrefs(@NonNull final SharedPreferences sharedPreferences,
                                                             @NonNull final String key) {
        Boolean valueToSynchronize = null;
        if (sharedPreferences.contains(key)) {
            try {
                valueToSynchronize = sharedPreferences.getBoolean(key, false);
            } catch (ClassCastException ex) {
                GLogger.wx(TAG, "Unexpected value type.", ex);
            }
        }

        if (valueToSynchronize != null) {
            putBoolean(key, valueToSynchronize);
            return true;
        }

        return remove(key) != null;
    }

    /**
     * Kick off initial data read. Once completed, it will allow all values
     * to be returned from memory. All operations must wait for this load
     * to complete.
     */
    private void loadFromDisk() {

        // There is no benefit to schedule this on Scheduler.io() due to synchronization,
        // i.e. all operations must wait for initial load to complete. Use Schedulers.single()
        // same way as commitToDisk. This helps avoid complications with unit testing (due to
        // not having a Looper in that context).
        Completable.fromAction(() -> {
            try {
                map = mapper.readValue(file, typeRef);
            } catch (IOException ex) {
                GLogger.wx(TAG, "Failed loading data from disk.", ex);
            } finally {
                loadedFromDisk.countDown();
            }
        }).subscribeOn(Schedulers.single())
                .subscribe(
                        () -> GLogger.i(TAG, "Successfully loaded persistent preference data."),
                        (throwable -> GLogger.ex(TAG, "Failed loading persistent preference data.", throwable)));
    }

    /**
     * Commits data to disk. Performed asynchronously after each change
     * to the in-memory data structure.
     */
    private void commitToDisk() {
        if (commitToDiskDisposable != null && !commitToDiskDisposable.isDisposed()) {
            commitToDiskDisposable.dispose();
        }

        // The action is observed on Schedulers.single(), which ensures
        // proper ordering of the commits. This is important to avoid data
        // corruption, if commits end up happening out-of-order.
        commitToDiskDisposable =
            Completable.fromAction(() -> mapper.writeValue(file, map))
                    .subscribeOn(Schedulers.single())
                    .subscribe(
                            () -> GLogger.i(TAG, "Successfully committed persistent preference data to local storage."),
                            (throwable -> GLogger.ex(TAG, "Failed committing persistent preference data to local storage.", throwable)));
    }

    /**
     * Helper method for waiting on initial migration to complete.
     * This is necessary to prevent incorrectly overwriting existing settings.
     */
    private void awaitLoadFromDisk() {
        try {
            loadedFromDisk.await();
        } catch (InterruptedException unused) {
            Thread.currentThread().interrupt();
        }
    }
}