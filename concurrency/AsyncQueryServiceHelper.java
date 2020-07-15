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

import android.app.IntentService;
import android.content.*;
import android.database.Cursor;
import android.net.Uri;
import android.os.Handler;
import android.os.Message;
import android.os.RemoteException;
import android.os.SystemClock;
import com.amazon.pim.logging.Logging;
import com.android.calendar.AsyncQueryService.Operation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

@Deprecated
public class AsyncQueryServiceHelper extends IntentService {
    private static final String TAG = "AsyncQuery";

    private static final PriorityQueue<OperationInfo> sWorkQueue =
        new PriorityQueue<OperationInfo>();

    protected Class<AsyncQueryService> mService = AsyncQueryService.class;

    protected static class OperationInfo implements Delayed {
        public int token; // Used for cancel
        public int op;
        public ContentResolver resolver;
        public Uri uri;
        public String authority;
        public Handler handler;
        public String[] projection;
        public String selection;
        public String[] selectionArgs;
        public String orderBy;
        public Object result;
        public Object cookie;
        public ContentValues values;
        public ArrayList<ContentProviderOperation> cpo;

        /**
         * delayMillis is relative time e.g. 10,000 milliseconds
         */
        public long delayMillis;

        /**
         * scheduleTimeMillis is the time scheduled for this to be processed.
         * e.g. SystemClock.elapsedRealtime() + 10,000 milliseconds Based on
         * {@link android.os.SystemClock#elapsedRealtime }
         */
        private long mScheduledTimeMillis = 0;

        // @VisibleForTesting
        void calculateScheduledTime() {
            mScheduledTimeMillis = SystemClock.elapsedRealtime() + delayMillis;
        }

        // @Override // Uncomment with Java6
        public long getDelay(TimeUnit unit) {
            return unit.convert(mScheduledTimeMillis - SystemClock.elapsedRealtime(),
                    TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed another) {
            OperationInfo anotherArgs = (OperationInfo) another;
            if (this.mScheduledTimeMillis == anotherArgs.mScheduledTimeMillis) {
                return 0;
            } else if (this.mScheduledTimeMillis < anotherArgs.mScheduledTimeMillis) {
                return -1;
            } else {
                return 1;
            }
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("OperationInfo [\n\t token= ");
            builder.append(token);
            builder.append(",\n\t op= ");
            builder.append(Operation.opToChar(op));
            builder.append(",\n\t uri= ");
            builder.append(uri);
            builder.append(",\n\t authority= ");
            builder.append(authority);
            builder.append(",\n\t delayMillis= ");
            builder.append(delayMillis);
            builder.append(",\n\t mScheduledTimeMillis= ");
            builder.append(mScheduledTimeMillis);
            builder.append(",\n\t resolver= ");
            builder.append(resolver);
            builder.append(",\n\t handler= ");
            builder.append(handler);
            builder.append(",\n\t projection= ");
            builder.append(Arrays.toString(projection));
            builder.append(",\n\t selection= ");
            builder.append(selection);
            builder.append(",\n\t selectionArgs= ");
            builder.append(Arrays.toString(selectionArgs));
            builder.append(",\n\t orderBy= ");
            builder.append(orderBy);
            builder.append(",\n\t result= ");
            builder.append(result);
            builder.append(",\n\t cookie= ");
            builder.append(cookie);
            builder.append(",\n\t values= ");
            builder.append(values);
            builder.append(",\n\t cpo= ");
            builder.append(cpo);
            builder.append("\n]");
            return builder.toString();
        }

        /**
         * Compares an user-visible operation to this private OperationInfo
         * object
         *
         * @param o operation to be compared
         * @return true if logically equivalent
         */
        public boolean equivalent(Operation o) {
            return o.token == this.token && o.op == this.op;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            OperationInfo that = (OperationInfo) o;

            if (delayMillis != that.delayMillis) {
                return false;
            }
            if (mScheduledTimeMillis != that.mScheduledTimeMillis) {
                return false;
            }
            if (op != that.op) {
                return false;
            }
            if (token != that.token) {
                return false;
            }
            if (authority != null ? !authority.equals(that.authority) : that.authority != null) {
                return false;
            }
            if (cookie != null ? !cookie.equals(that.cookie) : that.cookie != null) {
                return false;
            }
            if (cpo != null ? !cpo.equals(that.cpo) : that.cpo != null) {
                return false;
            }
            if (handler != null ? !handler.equals(that.handler) : that.handler != null) {
                return false;
            }
            if (orderBy != null ? !orderBy.equals(that.orderBy) : that.orderBy != null) {
                return false;
            }
            if (!Arrays.equals(projection, that.projection)) {
                return false;
            }
            if (resolver != null ? !resolver.equals(that.resolver) : that.resolver != null) {
                return false;
            }
            if (result != null ? !result.equals(that.result) : that.result != null) {
                return false;
            }
            if (selection != null ? !selection.equals(that.selection) : that.selection != null) {
                return false;
            }
            if (!Arrays.equals(selectionArgs, that.selectionArgs)) {
                return false;
            }
            if (uri != null ? !uri.equals(that.uri) : that.uri != null) {
                return false;
            }
            if (values != null ? !values.equals(that.values) : that.values != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result1 = token;
            result1 = 31 * result1 + op;
            result1 = 31 * result1 + (resolver != null ? resolver.hashCode() : 0);
            result1 = 31 * result1 + (uri != null ? uri.hashCode() : 0);
            result1 = 31 * result1 + (authority != null ? authority.hashCode() : 0);
            result1 = 31 * result1 + (handler != null ? handler.hashCode() : 0);
            result1 = 31 * result1 + (projection != null ? Arrays.hashCode(projection) : 0);
            result1 = 31 * result1 + (selection != null ? selection.hashCode() : 0);
            result1 = 31 * result1 + (selectionArgs != null ? Arrays.hashCode(selectionArgs) : 0);
            result1 = 31 * result1 + (orderBy != null ? orderBy.hashCode() : 0);
            result1 = 31 * result1 + (result != null ? result.hashCode() : 0);
            result1 = 31 * result1 + (cookie != null ? cookie.hashCode() : 0);
            result1 = 31 * result1 + (values != null ? values.hashCode() : 0);
            result1 = 31 * result1 + (cpo != null ? cpo.hashCode() : 0);
            result1 = 31 * result1 + (int) (delayMillis ^ (delayMillis >>> 32));
            result1 = 31 * result1 + (int) (mScheduledTimeMillis ^ (mScheduledTimeMillis >>> 32));
            return result1;
        }
    }

    /**
     * Queues the operation for execution
     *
     * @param context
     * @param args OperationInfo object describing the operation
     */
    static public void queueOperation(Context context, OperationInfo args) {
        // Set the schedule time for execution based on the desired delay.
        args.calculateScheduledTime();

        synchronized (sWorkQueue) {
            sWorkQueue.add(args);
            sWorkQueue.notify();
        }

        context.startService(new Intent(context, AsyncQueryServiceHelper.class));
    }

    /**
     * Gets the last delayed operation. It is typically used for canceling.
     *
     * @return Operation object which contains of the last cancelable operation
     */
    static public Operation getLastCancelableOperation() {
        long lastScheduleTime = Long.MIN_VALUE;
        Operation op = null;

        synchronized (sWorkQueue) {
            // Unknown order even for a PriorityQueue
            Iterator<OperationInfo> it = sWorkQueue.iterator();
            while (it.hasNext()) {
                OperationInfo info = it.next();
                if (info.delayMillis > 0 && lastScheduleTime < info.mScheduledTimeMillis) {
                    if (op == null) {
                        op = new Operation();
                    }

                    op.token = info.token;
                    op.op = info.op;
                    op.scheduledExecutionTime = info.mScheduledTimeMillis;

                    lastScheduleTime = info.mScheduledTimeMillis;
                }
            }
        }

        if (AsyncQueryService.localLOGV) {
            Logging.log(TAG, "getLastCancelableOperation -> Operation:"
                + Operation.opToChar(op.op) + " token:" + op.token);
        }
        return op;
    }

    /**
     * Attempts to cancel operation that has not already started. Note that
     * there is no guarantee that the operation will be canceled. They still may
     * result in a call to on[Query/Insert/Update/Delete/Batch]Complete after
     * this call has completed.
     *
     * @param token The token representing the operation to be canceled. If
     *            multiple operations have the same token they will all be
     *            canceled.
     */
    static public int cancelOperation(int token) {
        int canceled = 0;
        synchronized (sWorkQueue) {
            Iterator<OperationInfo> it = sWorkQueue.iterator();
            while (it.hasNext()) {
                if (it.next().token == token) {
                    it.remove();
                    ++canceled;
                }
            }
        }

        if (AsyncQueryService.localLOGV) {
            Logging.log(TAG, "cancelOperation(" + token + ") -> " + canceled);
        }
        return canceled;
    }

    public AsyncQueryServiceHelper(String name) {
        super(name);
    }

    public AsyncQueryServiceHelper() {
        super("AsyncQueryServiceHelper");
    }

    private boolean isUndoable(int op) {
        // Event query is not Undoable.
        return op != Operation.EVENT_ARG_QUERY;
    }

    @Override
    protected void onHandleIntent(Intent intent) {
        OperationInfo args;

        if (AsyncQueryService.localLOGV) {
            Logging.log(TAG, "onHandleIntent: queue size=" + sWorkQueue.size());
        }
        synchronized (sWorkQueue) {
            while (true) {
                /*
                 * This method can be called with no work because of
                 * cancellations
                 */
                if (sWorkQueue.size() == 0) {
                    return;
                } else if (sWorkQueue.size() == 1) {
                    OperationInfo first = sWorkQueue.peek();
                    long waitTime = first.mScheduledTimeMillis - SystemClock.elapsedRealtime();
                    // NOTE: SystemClock.elapsedRealtime() may go backwards sometimes and the
                    // code was meant to handle "Undo" operation, but did not get implemented.
                    if (isUndoable(first.op) && waitTime > 0) {
                        try {
                            sWorkQueue.wait(waitTime);
                        } catch (InterruptedException e) {
                        }
                    }
                }

                args = sWorkQueue.poll();
                if (args != null) {
                    // Got work to do. Break out of waiting loop
                    break;
                }
            }
        }

        if (AsyncQueryService.localLOGV) {
            Logging.log(TAG, "onHandleIntent: " + args);
        }

        ContentResolver resolver = args.resolver;
        if (resolver != null) {

            switch (args.op) {
                case Operation.EVENT_ARG_QUERY:
                    Cursor cursor;
                    try {
                        cursor = resolver.query(args.uri, args.projection, args.selection,
                                args.selectionArgs, args.orderBy);
                        /*
                         * Calling getCount() causes the cursor window to be
                         * filled, which will make the first access on the main
                         * thread a lot faster
                         */
                        if (cursor != null) {
                            cursor.getCount();
                        }
                    } catch (Exception e) {
                        Logging.warn(TAG, e.toString());
                        cursor = null;
                    }

                    args.result = cursor;
                    break;

                case Operation.EVENT_ARG_INSERT:
                    args.result = resolver.insert(args.uri, args.values);
                    break;

                case Operation.EVENT_ARG_UPDATE:
                    args.result = resolver.update(args.uri, args.values, args.selection,
                            args.selectionArgs);
                    break;

                case Operation.EVENT_ARG_DELETE:
                    args.result = resolver.delete(args.uri, args.selection, args.selectionArgs);
                    break;

                case Operation.EVENT_ARG_BATCH:
                    try {
                        args.result = resolver.applyBatch(args.authority, args.cpo);
                    } catch (RemoteException e) {
                        Logging.error(TAG, e.toString());
                        args.result = null;
                    } catch (OperationApplicationException e) {
                        Logging.error(TAG, e.toString());
                        args.result = null;
                    }
                    break;
                default:
                    break;
            }

            /*
             * passing the original token value back to the caller on top of the
             * event values in arg1.
             */
            Message reply = args.handler.obtainMessage(args.token);
            reply.obj = args;
            reply.arg1 = args.op;

            if (AsyncQueryService.localLOGV) {
                Logging.log(TAG, "onHandleIntent: op=" + Operation.opToChar(args.op)
                    + ", token=" + reply.what);
            }

            reply.sendToTarget();
        }
    }

    @Override
    public void onStart(Intent intent, int startId) {
        if (AsyncQueryService.localLOGV) {
            Logging.log(TAG, "onStart startId=" + startId);
        }
        super.onStart(intent, startId);
    }

    @Override
    public void onCreate() {
        if (AsyncQueryService.localLOGV) {
            Logging.log(TAG, "onCreate");
        }
        super.onCreate();
    }

    @Override
    public void onDestroy() {
        if (AsyncQueryService.localLOGV) {
            Logging.log(TAG, "onDestroy");
        }
        super.onDestroy();
    }
}