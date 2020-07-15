/*
 * Copyright (C) 2011 The Android Open Source Project
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

import android.os.AsyncTask;
import android.os.AsyncTask.Status;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link AsyncTask} substitution for the email app.
 *
 * Modeled after {@link AsyncTask}; the basic usage is the same, with extra features:
 * - Bulk cancellation of multiple tasks.  This is mainly used by UI to cancel pending tasks
 *   in onDestroy() or similar places.
 * - Instead of {@link AsyncTask#onPostExecute}, it has {@link #onSuccess(Object)}, as the
 *   regular {@link AsyncTask#onPostExecute} is a bit hard to predict when it'll be called and
 *   whel it won't.
 *
 * Note this class is missing some of the {@link AsyncTask} features, e.g. it lacks
 * {@link AsyncTask#onProgressUpdate}.  Add these when necessary.
 */
public abstract class EmailAsyncTask<Params, Progress, Result> {
    public static final String TAG = EmailAsyncTask.class.getSimpleName();

    private static final Executor SERIAL_EXECUTOR = AsyncTask.SERIAL_EXECUTOR;
    private static final Executor PARALLEL_EXECUTOR = AsyncTask.THREAD_POOL_EXECUTOR;
    protected static final int TASK_INCREASED_PRIORITY =
        android.os.Process.THREAD_PRIORITY_BACKGROUND
        + android.os.Process.THREAD_PRIORITY_MORE_FAVORABLE;
    private static CountDownLatch sCountDownLatch;
    private static AtomicInteger sTaskCount;
    /**
     * Tracks {@link EmailAsyncTask}.
     *
     * Call {@link #cancellAllInterrupt()} to cancel all tasks registered.
     */
    public static class Tracker {
        private final LinkedList<EmailAsyncTask<?, ?, ?>> mTasks =
                new LinkedList<EmailAsyncTask<?, ?, ?>>();

        private void add(EmailAsyncTask<?, ?, ?> task) {
            synchronized (mTasks) {
                mTasks.add(task);
            }
        }

        private void remove(EmailAsyncTask<?, ?, ?> task) {
            synchronized (mTasks) {
                mTasks.remove(task);
            }
        }

        /**
         * Cancel all registered tasks.
         */
        public void cancellAllInterrupt() {
            synchronized (mTasks) {
                for (EmailAsyncTask<?, ?, ?> task : mTasks) {
                    task.cancel(true);
                }
                mTasks.clear();
            }
        }

        /**
         * Cancel all instances of the same class as {@code current} other than
         * {@code current} itself.
         */
        /* package */ void cancelOthers(EmailAsyncTask<?, ?, ?> current) {
            final Class<?> clazz = current.getClass();
            synchronized (mTasks) {
                final ArrayList<EmailAsyncTask<?, ?, ?>> toRemove =
                        new ArrayList<EmailAsyncTask<?, ?, ?>>();
                for (EmailAsyncTask<?, ?, ?> task : mTasks) {
                    if ((task != current) && task.getClass().equals(clazz)) {
                        task.cancel(true);
                        toRemove.add(task);
                    }
                }
                for (EmailAsyncTask<?, ?, ?> task : toRemove) {
                    mTasks.remove(task);
                }
            }
        }

        public int getTaskCountForTest() {
            return mTasks.size();
        }

        /* package */ boolean containsTaskForTest(EmailAsyncTask<?, ?, ?> task) {
            return mTasks.contains(task);
        }
    }

    private final Tracker mTracker;

    private static class InnerTask<Params2, Progress2, Result2>
            extends AsyncTask<Params2, Progress2, Result2> {
        private final EmailAsyncTask<Params2, Progress2, Result2> mOwner;
        private boolean mBumpPriority;

        public InnerTask(EmailAsyncTask<Params2, Progress2, Result2> owner, boolean priority) {
            mOwner = owner;
            mBumpPriority = priority;
            increaseTaskCount();
        }

        @Override
        protected Result2 doInBackground(Params2... params) {
            if (mBumpPriority) {
                Thread.currentThread().setPriority(TASK_INCREASED_PRIORITY);
            }
            return mOwner.doInBackground(params);
        }

        @Override
        public void onCancelled(Result2 result) {
            mOwner.unregisterSelf();
            mOwner.onCancelled(result);
            countDown();
        }

        @Override
        public void onPostExecute(Result2 result) {
            mOwner.unregisterSelf();
            if (mOwner.mCancelled) {
                mOwner.onCancelled(result);
            } else {
                mOwner.onSuccess(result);
            }
            countDown();
        }
    }

    private final InnerTask<Params, Progress, Result> mInnerTask;
    private volatile boolean mCancelled;

    public EmailAsyncTask() {
        this(null, false);
    }

    public EmailAsyncTask(Tracker tracker) {
        this(tracker, false);
    }

    public EmailAsyncTask(Tracker tracker, boolean bumpPriority) {
        mTracker = tracker;
        if (mTracker != null) {
            mTracker.add(this);
        }
        mInnerTask = new InnerTask<Params, Progress, Result>(this, bumpPriority);
    }

    /* package */ final void unregisterSelf() {
        if (mTracker != null) {
            mTracker.remove(this);
        }
    }

    /** @see AsyncTask#doInBackground */
    protected abstract Result doInBackground(Params... params);


    /** @see AsyncTask#cancel(boolean) */
    public final void cancel(boolean mayInterruptIfRunning) {
        mCancelled = true;
        mInnerTask.cancel(mayInterruptIfRunning);
    }

    /** @see AsyncTask#onCancelled */
    protected void onCancelled(Result result) {
    }

    /** @see android.os.AsyncTask#isCancelled() */
    public final boolean isCancelled() {
        return mCancelled;
    }

    /**
     * Similar to {@link AsyncTask#onPostExecute}, but this will never be executed if
     * {@link #cancel(boolean)} has been called before its execution, even if
     * {@link #doInBackground(Object...)} has completed when cancelled.
     *
     * @see AsyncTask#onPostExecute
     */
    protected void onSuccess(Result result) {
    }

    /**
     * execute on {@link #PARALLEL_EXECUTOR}
     *
     * @see AsyncTask#execute
     */
    public final EmailAsyncTask<Params, Progress, Result> executeParallel(Params... params) {
        return executeInternal(PARALLEL_EXECUTOR, false, params);
    }

    /**
     * execute on {@link #SERIAL_EXECUTOR}
     *
     * @see AsyncTask#execute
     */
    public final EmailAsyncTask<Params, Progress, Result> executeSerial(Params... params) {
        return executeInternal(SERIAL_EXECUTOR, false, params);
    }

    /**
     * execute on {@link #SERIAL_EXECUTOR}. <br>
     * Consider if your scenario can cancel previous async task before starting a new one, since
     * creating a new async task every time will hit performance.<br>
     * If so, use {@link #cancelPreviousAndExecuteSerial(Object[])} or {@link #cancelPreviousAndExecuteParallel(Object[])}
     * based on your use case.
     *
     * @see AsyncTask#execute
     */
    public final EmailAsyncTask<Params, Progress, Result> execute(Params... params) {
        return executeSerial(params);
    }

    /**
     * Cancel all previously created instances of the same class tracked by the same
     * {@link Tracker}, and then {@link #executeParallel}.
     */
    public final EmailAsyncTask<Params, Progress, Result> cancelPreviousAndExecuteParallel(
            Params... params) {
        return executeInternal(PARALLEL_EXECUTOR, true, params);
    }

    /**
     * Cancel all previously created instances of the same class tracked by the same
     * {@link Tracker}, and then {@link #executeSerial}.
     */
    public final EmailAsyncTask<Params, Progress, Result> cancelPreviousAndExecuteSerial(
            Params... params) {
        return executeInternal(SERIAL_EXECUTOR, true, params);
    }

    private final EmailAsyncTask<Params, Progress, Result> executeInternal(Executor executor,
            boolean cancelPrevious, Params... params) {
        if (cancelPrevious) {
            if (mTracker == null) {
                throw new IllegalStateException();
            } else {
                mTracker.cancelOthers(this);
            }
        }
        mInnerTask.executeOnExecutor(executor, params);
        return this;
    }

    /**
     * Runs a {@link Runnable} in a bg thread, using {@link #PARALLEL_EXECUTOR}.
     */
    public static EmailAsyncTask<Void, Void, Void> runAsyncParallel(Runnable runnable) {
        return runAsyncInternal(PARALLEL_EXECUTOR, runnable);
    }

    /**
     * Runs a {@link Runnable} in a bg thread, using {@link #SERIAL_EXECUTOR}.
     */
    public static EmailAsyncTask<Void, Void, Void> runAsyncSerial(Runnable runnable) {
        return runAsyncInternal(SERIAL_EXECUTOR, runnable);
    }

    private static EmailAsyncTask<Void, Void, Void> runAsyncInternal(Executor executor,
            final Runnable runnable) {
        EmailAsyncTask<Void, Void, Void> task = new EmailAsyncTask<Void, Void, Void>(null) {
            @Override
            protected Void doInBackground(Void... params) {
                runnable.run();
                return null;
            }
        };
        return task.executeInternal(executor, false, (Void[]) null);
    }

    /**
     * Wait until {@link #doInBackground} finishes and returns the results of the computation.
     *
     * @see AsyncTask#get
     */
    public final Result get() throws InterruptedException, ExecutionException {
        return mInnerTask.get();
    }

    /**
     * Get the status of the inner task
     * @return
     */
    public Status getStatus() {
        return mInnerTask.getStatus();
    }

    /* package */ final Result callDoInBackgroundForTest(Params... params) {
        return mInnerTask.doInBackground(params);
    }

    /* package */ final void callOnCancelledForTest(Result result) {
        mInnerTask.onCancelled(result);
    }

    /* package */ final void callOnPostExecuteForTest(Result result) {
        mInnerTask.onPostExecute(result);
    }

    private static void countDown() {
        if (sCountDownLatch != null) {
            int count = sTaskCount.decrementAndGet();
            if (count == 0) {
                sCountDownLatch.countDown();
            }
        }
    }

    private static void increaseTaskCount() {
        if (sTaskCount != null) {
            sTaskCount.incrementAndGet();
        }
    }

    /**
     * Set up count down latch for unit tests
     * Note: Don't forget to reset it to null or call tearDown() after using it. The best way is
     * call tearDown in the TestCase.tearDown() function.
     * @param countDownLatch
     */
    public static void setCountDownLatch(CountDownLatch countDownLatch) {
        sCountDownLatch = countDownLatch;
        sTaskCount = new AtomicInteger();
    }

    /**
     * Reset the count down latch
     */
    public static void tearDown() {
        sCountDownLatch = null;
        sTaskCount = null;
    }
}