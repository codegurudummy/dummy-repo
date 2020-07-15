/*
 * Copyright (C) 2010 The Android Open Source Project
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

package concurrency;

import android.accounts.Account;
import android.content.ContentResolver;
import android.content.Context;
import android.content.pm.PackageManager;
import android.content.pm.PackageManager.NameNotFoundException;
import android.content.res.Resources;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.Handler;
import android.os.Message;
import android.provider.ContactsContract;
import android.provider.ContactsContract.Directory;
import android.provider.ContactsContract.DisplayNameSources;
import android.text.TextUtils;
import android.text.util.Rfc822Token;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Filter;
import android.widget.Filterable;
import android.widget.TextView;
import com.android.common.contacts.Queries.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

/**
 * A base class for email address / phone numbers autocomplete adapters. It uses
 * {@link Queries.Query.CONTENT_FILTER_URI} to search for data rows by email address or phone number
 * and/or contact name. It also searches registered {@link Directory}'s. This class is just a copy
 * of frameworks/ex_common_java with a modification to abstract out the part to decide which query
 * to choose the e-mail one or the phone one.
 */
public class BaseFilterAdapter extends CompositeCursorAdapter implements Filterable {
    private static final String TAG = "BaseEmailAddressAdapter";

    // TODO: revert to references to the Directory class as soon as the
    // issue with the dependency on SDK 8 is resolved

    // This is Directory.LOCAL_INVISIBLE
    private static final long DIRECTORY_LOCAL_INVISIBLE = 1;

    // This is ContactsContract.DIRECTORY_PARAM_KEY
    private static final String DIRECTORY_PARAM_KEY = "directory";

    // This is ContactsContract.LIMIT_PARAM_KEY
    private static final String LIMIT_PARAM_KEY = "limit";

    // This is ContactsContract.PRIMARY_ACCOUNT_NAME
    private static final String PRIMARY_ACCOUNT_NAME = "name_for_primary_account";
    // This is ContactsContract.PRIMARY_ACCOUNT_TYPE
    private static final String PRIMARY_ACCOUNT_TYPE = "type_for_primary_account";

    /**
     * The preferred number of results to be retrieved. This number may be exceeded if there are
     * several directories configured, because we will use the same limit for all directories.
     */
    private static final int DEFAULT_PREFERRED_MAX_RESULT_COUNT = 10;

    /**
     * The number of extra entries requested to allow for duplicates. Duplicates are removed from
     * the overall result.
     */
    private static final int ALLOWANCE_FOR_DUPLICATES = 5;

    /**
     * The "Searching..." message will be displayed if search is not complete within this many
     * milliseconds.
     */
    private static final int MESSAGE_SEARCH_PENDING_DELAY = 1000;

    private static final int MESSAGE_SEARCH_PENDING = 1;

    private static final int MAX_PARALLEL_QUERY_PERMITS = 8;
    // The Counting semaphore used to keep track of the current executing GAL queries.Limiting to
    // 8 outstanding queries
    private final Semaphore mParallelQueryPermits = new Semaphore(MAX_PARALLEL_QUERY_PERMITS);
    //The Queue for all the Gal query requests
    private final Queue<DirectoryPartition> mPendingDirectoryQueryQueue =
        new ConcurrentLinkedQueue<DirectoryPartition>();
    /**
     * Model object for a {@link Directory} row. There is a partition in the
     * {@link CompositeCursorAdapter} for every directory (except {@link Directory#LOCAL_INVISIBLE}.
     */
    public final static class DirectoryPartition extends CompositeCursorAdapter.Partition {
        public long directoryId;
        public String directoryType;
        public String displayName;
        public String accountName;
        public String accountType;
        public boolean loading;
        public CharSequence constraint;
        public DirectoryPartitionFilter filter;

        public DirectoryPartition() {
            super(false, false);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj == this) {
                return true;
            }
            if (obj instanceof DirectoryPartition) {
                return directoryId == ((DirectoryPartition) obj).directoryId;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return (int) directoryId;
        }
    }

    private static class DirectoryListQuery {

        // TODO: revert to references to the Directory class as soon as the
        // issue with the dependency on SDK 8 is resolved
        public static final Uri URI = Uri.withAppendedPath(ContactsContract.AUTHORITY_URI,
            "directories");
        private static final String DIRECTORY_ID = "_id";
        private static final String DIRECTORY_ACCOUNT_NAME = "accountName";
        private static final String DIRECTORY_ACCOUNT_TYPE = "accountType";
        private static final String DIRECTORY_DISPLAY_NAME = "displayName";
        private static final String DIRECTORY_PACKAGE_NAME = "packageName";
        private static final String DIRECTORY_TYPE_RESOURCE_ID = "typeResourceId";

        public static final String[] PROJECTION = { DIRECTORY_ID, // 0
            DIRECTORY_ACCOUNT_NAME, // 1
            DIRECTORY_ACCOUNT_TYPE, // 2
            DIRECTORY_DISPLAY_NAME, // 3
            DIRECTORY_PACKAGE_NAME, // 4
            DIRECTORY_TYPE_RESOURCE_ID, // 5
        };

        public static final int ID = 0;
        public static final int ACCOUNT_NAME = 1;
        public static final int ACCOUNT_TYPE = 2;
        public static final int DISPLAY_NAME = 3;
        public static final int PACKAGE_NAME = 4;
        public static final int TYPE_RESOURCE_ID = 5;
    }

    /**
     * A fake column name that indicates a "Searching..." item in the list.
     */
    private static final String SEARCHING_CURSOR_MARKER = "searching";

    /**
     * An asynchronous filter used for loading two data sets: email rows from the local contact
     * provider and the list of {@link Directory}'s.
     */
    private final class DefaultPartitionFilter extends Filter {

        @Override
        protected FilterResults performFiltering(CharSequence constraint) {
            if (constraint == null) {
                return null;
            }
            Cursor directoryCursor = null;
            if (!mDirectoriesLoaded) {
                directoryCursor = mContentResolver.query(DirectoryListQuery.URI,
                    DirectoryListQuery.PROJECTION, null, null, null);
                mDirectoriesLoaded = true;
            }

            FilterResults results = new FilterResults();
            Cursor cursor = null;
            if (!TextUtils.isEmpty(constraint)) {
                Uri.Builder builder = mQuery
                    .getContentFilterUri()
                    .buildUpon()
                    .appendPath(constraint.toString())
                    .appendQueryParameter(LIMIT_PARAM_KEY, String.valueOf(mPreferredMaxResultCount))
                    .appendQueryParameter(Query.PARAMETER_SHOW_HIDDEN, Boolean.toString(true));
                if (mAccount != null) {
                    builder.appendQueryParameter(PRIMARY_ACCOUNT_NAME, mAccount.name);
                    builder.appendQueryParameter(PRIMARY_ACCOUNT_TYPE, mAccount.type);
                }
                Uri uri = builder.build();
                cursor = mContentResolver.query(uri, mQuery.getProjection(), null, null, null);
                results.count = cursor.getCount();
            }
            results.values = new Cursor[] { directoryCursor, cursor };
            return results;
        }

        @Override
        protected void publishResults(CharSequence constraint, FilterResults results) {
            if (results != null) {
                if (results.values != null) {
                    Cursor[] cursors = (Cursor[]) results.values;
                    onDirectoryLoadFinished(constraint, cursors[0], cursors[1]);
                }
                results.count = getCount();
            }
        }

        @Override
        public CharSequence convertResultToString(Object resultValue) {
            return makeDisplayString((Cursor) resultValue);
        }
    }

    /**
     * An asynchronous filter that performs search in a particular directory.
     */
    private final class DirectoryPartitionFilter extends Filter {
        private final int mPartitionIndex;
        private final long mDirectoryId;
        private int mLimit;

        public DirectoryPartitionFilter(int partitionIndex, long directoryId) {
            this.mPartitionIndex = partitionIndex;
            this.mDirectoryId = directoryId;
        }

        public synchronized void setLimit(int limit) {
            this.mLimit = limit;
        }

        public synchronized int getLimit() {
            return this.mLimit;
        }

        @Override
        protected FilterResults performFiltering(CharSequence constraint) {
            FilterResults results = new FilterResults();
            if (!TextUtils.isEmpty(constraint)) {
                Uri uri = mQuery
                    .getContentFilterUri()
                    .buildUpon()
                    .appendPath(constraint.toString())
                    .appendQueryParameter(DIRECTORY_PARAM_KEY, String.valueOf(mDirectoryId))
                    .appendQueryParameter(LIMIT_PARAM_KEY,
                        String.valueOf(getLimit() + ALLOWANCE_FOR_DUPLICATES)).build();
                Cursor cursor = mContentResolver.query(uri, mQuery.getProjection(), null, null,
                    null);
                results.values = cursor;
            }
            return results;
        }

        @Override
        protected void publishResults(CharSequence constraint, FilterResults results) {
            // Release the GAL query permit
            mParallelQueryPermits.release();
            //Kick off the queue processing to see if there are any pending requests.
            processGalQueryQueue();
            Cursor cursor = (Cursor) results.values;
            onPartitionLoadFinished(constraint, mPartitionIndex, cursor);
            results.count = getCount();
        }
    }

    private static final class Holder {
        TextView nameView;
        TextView dataView;
    }

    protected final ContentResolver mContentResolver;
    private ContactListConfiguration mConfig;
    private boolean mDirectoriesLoaded;
    private Account mAccount;
    private int mPreferredMaxResultCount;
    private final Handler mHandler = new Handler() {
        @Override
        public void handleMessage(Message msg) {
            showSearchPendingIfNotComplete(msg.arg1);
        }
    };
    protected LayoutInflater mInflater;

    private Query mQuery;

    public BaseFilterAdapter(Context context) {
        this(context, DEFAULT_PREFERRED_MAX_RESULT_COUNT);
    }

    public BaseFilterAdapter(Context context, int preferredMaxResultCount) {
        super(context);
        mContentResolver = context.getContentResolver();
        mPreferredMaxResultCount = preferredMaxResultCount;
        mInflater = LayoutInflater.from(context);
    }

    public void setContactListConfiguration(ContactListConfiguration config) {
        mConfig = config;
    }

    /**
     * Set the account when known. Causes the search to prioritize contacts from that account.
     */
    public void setAccount(Account account) {
        mAccount = account;
    }

    protected void setQuery(Query query) {
        mQuery = query;
    }

    @Override
    protected int getItemViewType(int partitionIndex, int position) {
        DirectoryPartition partition = (DirectoryPartition) getPartition(partitionIndex);
        return partition.loading ? 1 : 0;
    }

    @Override
    protected View newView(Context context, int partitionIndex, Cursor cursor, int position,
        ViewGroup parent) {
        DirectoryPartition partition = (DirectoryPartition) getPartition(partitionIndex);
        View view = null;
        if (partition.loading) {
            view = mInflater.inflate(R.layout.recipient_dropdown_item_loading, parent, false);
        } else {
            view = mInflater.inflate(R.layout.recipient_dropdown_item, parent, false);
        }
        if (mConfig != null) {
            TextView tv = (TextView) view.findViewById(R.id.text1);
            if (tv != null) {
                mConfig.setDropdownMainTextStyle(tv);
            }
            tv = (TextView) view.findViewById(R.id.text2);
            if (tv != null) {
                mConfig.setDropdownSubTextStyle(tv);
            }
        }
        return view;
    }

    @Override
    protected void bindView(View v, int partition, Cursor cursor, int position) {

        Holder holder = (Holder) v.getTag();
        if (holder == null) {
            holder = new Holder();
            holder.nameView = (TextView) v.findViewById(R.id.text1);
            holder.dataView = (TextView) v.findViewById(R.id.text2);
            v.setTag(holder);
        }

        DirectoryPartition directoryPartition = (DirectoryPartition) getPartition(partition);
        String directoryType = directoryPartition.directoryType;
        String directoryName = directoryPartition.displayName;
        if (directoryPartition.loading) {
            String text = getContext().getString(R.string.gal_searching_fmt,
                TextUtils.isEmpty(directoryName) ? directoryType : directoryName);
            holder.nameView.setText(text);
        } else {
            String displayName = cursor.getString(Query.DISPLAY_NAME);
            String emailAddress = cursor.getString(Query.CONTENT_DATA);
            if (TextUtils.isEmpty(displayName) || TextUtils.equals(displayName, emailAddress)) {
                displayName = emailAddress;
            }
            holder.nameView.setText(displayName);
            holder.dataView.setText(emailAddress);
        }
    }

    @Override
    public boolean areAllItemsEnabled() {
        return false;
    }

    @Override
    protected boolean isEnabled(int partitionIndex, int position) {
        // The "Searching..." item should not be selectable
        return !isLoading(partitionIndex);
    }

    private boolean isLoading(int partitionIndex) {
        return ((DirectoryPartition) getPartition(partitionIndex)).loading;
    }

    @Override
    public Filter getFilter() {
        return new DefaultPartitionFilter();
    }

    /**
     * Handles the result of the initial call, which brings back the list of directories as well as
     * the search results for the local directories.
     */
    protected void onDirectoryLoadFinished(CharSequence constraint, Cursor directoryCursor,
        Cursor defaultPartitionCursor) {
        if (directoryCursor != null) {
            PackageManager packageManager = getContext().getPackageManager();
            DirectoryPartition preferredDirectory = null;
            List<DirectoryPartition> directories = new ArrayList<DirectoryPartition>();
            while (directoryCursor.moveToNext()) {
                long id = directoryCursor.getLong(DirectoryListQuery.ID);

                // Skip the local invisible directory, because the default directory
                // already includes all local results.
                if (id == DIRECTORY_LOCAL_INVISIBLE) {
                    continue;
                }

                DirectoryPartition partition = new DirectoryPartition();
                partition.directoryId = id;
                partition.displayName = directoryCursor.getString(DirectoryListQuery.DISPLAY_NAME);
                partition.accountName = directoryCursor.getString(DirectoryListQuery.ACCOUNT_NAME);
                partition.accountType = directoryCursor.getString(DirectoryListQuery.ACCOUNT_TYPE);
                String packageName = directoryCursor.getString(DirectoryListQuery.PACKAGE_NAME);
                int resourceId = directoryCursor.getInt(DirectoryListQuery.TYPE_RESOURCE_ID);
                if (packageName != null && resourceId != 0) {
                    try {
                        Resources resources = packageManager
                            .getResourcesForApplication(packageName);
                        partition.directoryType = resources.getString(resourceId);
                        if (partition.directoryType == null) {
                            Log.e(TAG, "Cannot resolve directory name: " + resourceId + "@"
                                + packageName);
                        }
                    } catch (NameNotFoundException e) {
                        Log.e(TAG, "Cannot resolve directory name: " + resourceId + "@"
                            + packageName, e);
                    }
                }

                // If an account has been provided and we found a directory that
                // corresponds to that account, place that directory second, directly
                // underneath the local contacts.
                if (mAccount != null && mAccount.name.equals(partition.accountName)
                    && mAccount.type.equals(partition.accountType)) {
                    preferredDirectory = partition;
                } else {
                    directories.add(partition);
                }
            }

            if (preferredDirectory != null) {
                directories.add(1, preferredDirectory);
            }

            for (DirectoryPartition partition : directories) {
                addPartition(partition);
            }
        }

        int count = getPartitionCount();
        int limit = 0;

        // Since we will be changing several partitions at once, hold the data change
        // notifications
        setNotificationsEnabled(false);
        try {
            // The filter has loaded results for the default partition too.
            if (defaultPartitionCursor != null && getPartitionCount() > 0) {
                changeCursor(0, defaultPartitionCursor);
            }

            int defaultPartitionCount = (defaultPartitionCursor == null ? 0
                : defaultPartitionCursor.getCount());

            limit = mPreferredMaxResultCount - defaultPartitionCount;

            // Show non-default directories as "loading"
            // Note: skipping the default partition (index 0), which has already been loaded
            for (int i = 1; i < count; i++) {
                DirectoryPartition partition = (DirectoryPartition) getPartition(i);
                partition.constraint = constraint;

                if (limit > 0) {
                    if (!partition.loading) {
                        partition.loading = true;
                        changeCursor(i, null);
                    }
                } else {
                    partition.loading = false;
                    changeCursor(i, null);
                }
            }
        } finally {
            setNotificationsEnabled(true);
        }

        // Start search in other directories
        // Note: skipping the default partition (index 0), which has already been loaded
        for (int i = 1; i < count; i++) {
            DirectoryPartition partition = (DirectoryPartition) getPartition(i);
            if (partition.loading) {
                mHandler.removeMessages(MESSAGE_SEARCH_PENDING, partition);
                Message msg = mHandler.obtainMessage(MESSAGE_SEARCH_PENDING, i, 0, partition);
                mHandler.sendMessageDelayed(msg, MESSAGE_SEARCH_PENDING_DELAY);
                partition.filter = new DirectoryPartitionFilter(i, partition.directoryId);
                partition.filter.setLimit(limit);
                // Add the query to queue. IF the queue already has an entry for this directory,
                // the constraint is updated. No need to add it again.
                if (!mPendingDirectoryQueryQueue.contains(partition)) {
                    mPendingDirectoryQueryQueue.add(partition);
                }
                //Kick off the processing of the queue
                processGalQueryQueue();
            }
        }
    }

    // Helper method to process the Gal queries. First checked if there are any available permits,
    // and schedules a query only if there are.
    private void processGalQueryQueue() {
        while (!mPendingDirectoryQueryQueue.isEmpty()) {
            if (mParallelQueryPermits.tryAcquire()) {
                DirectoryPartition partition = mPendingDirectoryQueryQueue.remove();
                partition.filter.filter(partition.constraint);
            } else {
                //Stop processing until the a permit is available.
                break;
            }
        }
    }

    void showSearchPendingIfNotComplete(int partitionIndex) {
        if (partitionIndex < getPartitionCount()) {
            DirectoryPartition partition = (DirectoryPartition) getPartition(partitionIndex);
            if (partition.loading) {
                changeCursor(partitionIndex, createLoadingCursor());
            }
        }
    }

    /**
     * Creates a dummy cursor to represent the "Searching directory..." item.
     */
    private Cursor createLoadingCursor() {
        MatrixCursor cursor = new MatrixCursor(new String[] { SEARCHING_CURSOR_MARKER });
        cursor.addRow(new Object[] { "" });
        return cursor;
    }

    public void onPartitionLoadFinished(CharSequence constraint, int partitionIndex, Cursor cursor) {
        if (partitionIndex < getPartitionCount()) {
            DirectoryPartition partition = (DirectoryPartition) getPartition(partitionIndex);

            // Check if the received result matches the current constraint
            // If not - the user must have continued typing after the request
            // was issued
            if (partition.loading && TextUtils.equals(constraint, partition.constraint)) {
                partition.loading = false;
                mHandler.removeMessages(MESSAGE_SEARCH_PENDING, partition);
                changeCursor(partitionIndex, removeDuplicatesAndTruncate(partitionIndex, cursor));
            } else {
                // We got the result for an unexpected query (the user is still typing)
                // Just ignore this result
                if (cursor != null) {
                    cursor.close();
                }
            }
        } else if (cursor != null) {
            cursor.close();
        }
    }

    /**
     * Post-process the cursor to eliminate duplicates. Closes the original cursor and returns a new
     * one.
     */
    private Cursor removeDuplicatesAndTruncate(int partition, Cursor cursor) {
        if (cursor == null) {
            return null;
        }

        int count = 0;
        MatrixCursor newCursor = new MatrixCursor(mQuery.getProjection());
        cursor.moveToPosition(-1);
        while (cursor.moveToNext() && count < DEFAULT_PREFERRED_MAX_RESULT_COUNT) {
            String displayName = cursor.getString(Query.DISPLAY_NAME);
            String emailAddress = cursor.getString(Query.CONTENT_DATA);
            if (!isDuplicate(emailAddress, partition)) {
                newCursor.addRow(new Object[] { displayName, DisplayNameSources.UNDEFINED,
                    emailAddress, mQuery.getDefaultContentType(), null, ContactEntry.GENERATED_ID,
                    ContactEntry.GENERATED_ID, ContactEntry.ID_UNKNOWN });
                count++;
            }
        }
        cursor.close();

        return newCursor;
    }

    /**
     * Checks if the supplied email address is already present in partitions other than the supplied
     * one.
     */
    private boolean isDuplicate(String emailAddress, int excludePartition) {
        int partitionCount = getPartitionCount();
        for (int partition = 0; partition < partitionCount; partition++) {
            if (partition != excludePartition && !isLoading(partition)) {
                Cursor cursor = getCursor(partition);
                if (cursor != null) {
                    cursor.moveToPosition(-1);
                    while (cursor.moveToNext()) {
                        String address = cursor.getString(Query.CONTENT_DATA);
                        if (TextUtils.equals(emailAddress, address)) {
                            return true;
                        }
                    }
                }
            }
        }

        return false;
    }

    private final String makeDisplayString(Cursor cursor) {
        if (cursor.getColumnName(0).equals(SEARCHING_CURSOR_MARKER)) {
            return "";
        }

        String displayName = cursor.getString(Query.DISPLAY_NAME);
        String emailAddress = cursor.getString(Query.CONTENT_DATA);
        if (TextUtils.isEmpty(displayName) || TextUtils.equals(displayName, emailAddress)) {
            return emailAddress;
        } else {
            return new Rfc822Token(displayName, emailAddress, null).toString();
        }
    }
}