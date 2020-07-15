package concurrency;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * An implementation of a double-ended stack. Low priority jobs will go
 * into the bottom of the stack and the normal jobs go into the top of
 * the stack. The items from the top of the stack are provided to the
 * client and hence the later requests put in this stack will be generally
 * served first.
 *
 * @param <T> the type of metadata handled by this stack
 * @author harig
 */
public class JobStack<T extends AbstractMetadata> {
    public static interface Keyed {
        public String getKey();
    }

    private static interface IContextQueue<T extends AbstractMetadata> {
        public boolean remove(java.lang.Object jobContext);

        public void addFirst(JobContext<T> jobContext);

        public void addLast(JobContext<T> jobContext);

        public int size();

        public void clear();

        public JobContext<T> take() throws InterruptedException;

        public boolean isEmpty();
    }

    // For post-gingerbread devices where the linked blocking dequeue exists, use that
    private static class LBDContextQueue<T extends AbstractMetadata> extends LinkedBlockingDeque<JobContext<T>>
        implements IContextQueue<T> {
        static final long serialVersionUID = 1L;
    }

    private static class PBQContextQueue<T extends AbstractMetadata> extends PriorityBlockingQueue<JobContext<T>>
        implements IContextQueue<T> {
        static final long serialVersionUID = 1L;

        public void addFirst(JobContext<T> tJobContext) {
            // tweak this item's priority to push it to the front of the queue
            JobContext<T> peek = peek();
            if (peek != null) {
                tJobContext.setPriority(peek.getPriority() - 1);
            } else {
                tJobContext.setPriority(tJobContext.getPriority() - 1);
            }
            add(tJobContext);
        }

        public void addLast(JobContext<T> tJobContext) {
            add(tJobContext);
        }
    }

    //private LinkedBlockingDeque<JobContext<T>> mTaskQueue = new LinkedBlockingDeque<JobContext<T>>();
    private IContextQueue<T> mTaskQueue = new LBDContextQueue<T>();
    private HashMap<String, JobContext<T>> mTaskMap = new HashMap<String, JobContext<T>>();

    /**
     * This method is used to put job requests into stack.
     *
     * @param metadata the metadata for the job
     * @param listener the listener to notify when job state changes
     * @return the context of the job
     */
    public JobContext<T> put(T metadata, JobListener<T> listener) {
        JobContext<T> imageLoadContext = null;
        String key = metadata.getKey();
        if (key != null) {
            synchronized (mTaskMap) {
                if (mTaskMap.containsKey(key)) {
                    imageLoadContext = mTaskMap.get(key);
                    imageLoadContext.addListener(listener);
                    if (!metadata.isPrecacheRequest()) {
                        imageLoadContext.getMetadata().setPrecacheRequest(false);
                    }
                    if (!metadata.isLowPriority()) {
                        imageLoadContext.getMetadata().setLowPriority(false);
                        if (mTaskQueue.remove(imageLoadContext)) {
                            mTaskQueue.addFirst(imageLoadContext);
                        }
                    }
                } else if (!metadata.isUpdateOnly()) {
                    imageLoadContext = new JobContext<T>(metadata);
                    imageLoadContext.addListener(listener);
                    mTaskMap.put(key, imageLoadContext);
                    if (metadata.isLowPriority()) {
                        mTaskQueue.addLast(imageLoadContext);
                    } else {
                        mTaskQueue.addFirst(imageLoadContext);
                    }
                }
            }
        }
        return imageLoadContext;
    }

    /**
     * This method will provide the next job to be executed. The
     * call to this method will block until a job is available.
     *
     * @return the next job to be executed
     */
    public JobContext<T> take() throws InterruptedException {
        return mTaskQueue.take();
    }

    /**
     * Marks the job represented by the give job context as complete.
     *
     * @param jobContext the job context
     */
    public void markComplete(JobContext<T> jobContext) {
        synchronized (mTaskMap) {
            mTaskMap.remove(jobContext.getMetadata().getKey());
        }
    }

    /**
     * Checks if the stack is empty or not.
     *
     * @return true if the stack is empty.
     */
    public boolean isEmpty() {
        synchronized (mTaskMap) {
            return mTaskQueue.isEmpty();
        }
    }

    /**
     * Empty out the job stack
     */
    public void clearStack() {
        mTaskQueue.clear();
        mTaskMap.clear();
    }

    /**
     * Get the list of still running tasks
     */
    public int getTaskCount() {
        return mTaskMap.size();
    }

    public int getRemainingCount() {
        return mTaskQueue.size();
    }
}