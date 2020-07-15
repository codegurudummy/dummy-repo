package concurrency;

import com.amazon.bigbird.collections.SequenceRange;
import com.amazon.bigbird.logging.BigBirdFatalInterceptor;
import s3.commons.log.S3Logger;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazon.bigbird.exceptions.BigBirdAssert.bbAssert;

/**
 * Created by Kenny.
 * A queue which is intended to take live, ordered data through offer() and cache it up to a weight limit; after which
 *   the logReader callback is invoked which is intended to read additional entries from storage until
 *   current offer()'s are ordered according to the validate callback.
 * Consider a replication log on a live storage master (a producer) and a feature which uses these log
 *   entries asynchronously (a consumer).  It is advantageous to block neither, but to bound the memory
 *   queue for the consumer, while retaining the ability to completely poll the replication log by
 *   the consumer in the correct order.  This is a problem which this class helps address.
 * The logReader callback is used after an accepter callback during add has failed AND the backing cache is 1/2 empty.
 * maxWeight is a rough upper bound.  Use something else if you need byte-level max guarantees; this class
 *   favors simplicity over strictness in this category.
 *
 * The internal invocations by this class of the LogReader and WeightDeterminer are thread safe;
 *   however, externally these callbacks must be able to be called at any time from any thread and produce correct results.
 *
 * Each LogBackedQueue will only have 1 thread using the LogReader.
 *
 * It is explicitly expected that the usage pattern for this data structure is via external polling.  Periodically
 *   check whether there is anything in the queue.
 */
public class LogBackedQueue<T extends SequenceRange> implements AutoCloseable
{
    S3Logger log = new BigBirdFatalInterceptor();
    /**
     * A LogReader reads from some log which is probably larger than system memory to fill this structure when
     *  in-memory adds fail (due to memory constraint or ordering violations).
     * The log is treated as the absolute truth for the stream of added entries.
     * When readLog() is invoked, the entry immediately following lastRead must be the first entry for the returned
     *  iterable.
     * The reader should return a "small" batch size, something as efficient and atomic as possible for the underlying
     *  log strategy.  (I.e., a block of entries or a small count)
     */
    public interface LogReader<T>
    {
        /**
         * @param lastRead The last entry in the queue; read from the log exclusively following this entry.
         * @return The log entries immediately following lastRead, in a "small" batch size.
         */
        public List<T> readLogs(Long lastRead);
    }
    
    /**
     * Implementations define what notion of per-item weight is to be used.
     * Could be as simple as {return 1;} for an item count limit, or as cool as {return item.getInMemorySize();}
     *  for memory-bound implementations.
     * Pending Java8, this is a @FunctionalInterface.
     */
    public interface SizeDeterminer<T>
    {
        /**
         * @param item The item which we need the weight of.
         * @return The size of item.
         */
        public int getSize(T item);
    }

    public interface Synchronizer
    {
        /**
         * @param lastAcceptedSeq The most recent entry which has been added to the queue.
         * 
         * @return True if the LogBackedQueue should still be faulted to disk.
         */
        public boolean isFaulted(Long lastAcceptedSeq);
        
        /**
         * Lock function to synchronize actions on the queue. This is useful as the queue represents a 
         * filtered view for replication log for each session.
         */
        public void lock();
        
        /**
         * Unlock function on the queue.
         */
        public void unlock();
        
    }

    private final int maxSize; // Rough upper bound to the size this data structure will hold in memory.
    private final double recentQueueRatio; // Percentage of size which can be allocated to recent window for joining current traffic.
    private final double logReadRatio; // Target percentage of open space in buffer before reading a batch of entries from disk.
    private final Synchronizer synchronizer;
    private final long rangeStart; // Starting range from which the queue will work forward over the log.
    private final LogReader<T> logReader;
    private final SizeDeterminer<? super T> sizeDeterminer;
    private final AtomicInteger pollQueueSize;
    private final AtomicInteger recentQueueSize;
    private final AtomicBoolean faultedToLog;
    private final AtomicBoolean readingFromLog;
    private final AtomicBoolean isOpen;
    private final Executor logReaderExecutor;

    //This variable marks the last accepted sequence into the poll queue.
    private volatile Long lastAcceptedSeq;
    private volatile T lastCached;

    // This queue is used in the normal case, and is the one which is polled from.  Offered entries first try to go in here.
    private final ConcurrentLinkedQueue<T> pollQueue;

    // This queue is used to buffer recent offers which are out of context with the consistent poll queue.
    // It is a window of recent traffic, moving forward with traffic and discarding oldest entries as it goes.
    // Once the pollQueue has caught up to this recent window's range, it is joined into the pollQueue.
    //  After this, the recentOfferQueue goes unused until the next time we fault to disk.
    private final ConcurrentLinkedQueue<T> recentOfferQueue;
    
    /**
     * @param maxSize The rough max in-memory size for this queue.
     * @param logReader A callback which generates additional entries from storage.
     * @param sizeDeterminer A callback which indicates the "size" of an entry to cache; likely used to determine memory or absolute item cost.
     * @param logReaderExecutor The executor which runs the logReader invocations.
     * @param rangeStart The starting sequence for this queue.  The queue will allow polling from this sequence forward.
     */
    public LogBackedQueue(int maxSize, LogReader<T> logReader,
        SizeDeterminer<? super T> sizeDeterminer,
        Synchronizer synchronizer, Executor logReaderExecutor,
        long rangeStart, double recentQueueRatio, double logReadRatio)
    {
        this.maxSize = maxSize;
        this.logReader = logReader;
        this.sizeDeterminer = sizeDeterminer;
        this.synchronizer = synchronizer;
        this.rangeStart = rangeStart;
        this.pollQueue = new ConcurrentLinkedQueue<>();
        this.recentOfferQueue = new ConcurrentLinkedQueue<>();
        this.pollQueueSize = new AtomicInteger();
        this.recentQueueSize = new AtomicInteger();
        this.faultedToLog = new AtomicBoolean(true);
        this.readingFromLog = new AtomicBoolean(false);
        this.isOpen = new AtomicBoolean(true);
        this.logReaderExecutor = logReaderExecutor;
        this.recentQueueRatio = recentQueueRatio;
        this.logReadRatio = logReadRatio;
    }

    public long getRangeStart(){
        return rangeStart;
    }
    
    /**
     * @return The current size held by this queue, as defined by the SizeDeterminer.
     */
    public int getQueuedSize()
    {
        return pollQueueSize.get() + recentQueueSize.get();
    }

    /**
     * @return true if this queue will consult the log next rather than validate in-memory offers.
     */
    public boolean isFaultedTolog()
    {
        return faultedToLog.get();
    }

    public T peek()
    {
        // The use of this queue is via polling - we use any state inspection as an opportunity to update state.
        checkState();
        return pollQueue.peek();
    }

    public T poll()
    {
        T item = pollQueue.poll();
        removed(item);
        return item;
    }

    /**
     * @return Current size of the queue.  Not intended for polling, this does not queue disk accesses.
     * NOTE: This is queue length and not memory size.
     */
    public int size()
    {
        return pollQueue.size();
    }

    /**
     * Calling this method will result in an asynchronous disk read if the LogBackedQueue hasn't had any contents yet,
     *  or if it is faulted to disk and there's space for more entries.  Normally, this should only be called during
     *  eager initialization.
     */
    public void checkLog()
    {
        checkState();
    }

    /**
     * Attempts to add entry to the cache for iterating, subject to maxSize and the entries' ranges.
     * This method is synchronized around the accepter callback and the addition to the background cache to ensure ordering correctness.
     * @param entry The entry to add, assuming the cache has space and entry is ok to add according to the accepter callback.
     * @return It returns true when an entry is added to pollQueue; and false if this entry is cached in recentOfferQueue.
     */
    public boolean offer(T entry)
    {
        log.debug("offer", "offered", "entry.seq", entry.getRangeEnd());
        synchronizer.lock();
        try {
            // if the LBQ's size is over the predefined threshold, we add an entry to recentOfferQueue
            if (pollQueueSize.get() + recentQueueSize.get() >= maxSize) {
                faultedToLog.set(true);
                cacheRecentOffer(entry);
                return false;
            }

            // pollQueue still has available space and we try to add this entry to pollQueue is possible
            boolean inOrder = acceptOrdered(entry);
            if(inOrder) {
                return true;
            } else {
                // NOTE: only the poll queue is a real queue. We don't return true if it was just cached. The cache is used only for
                // enabling a join later. This is why we set faultedToLog as well because we are going to hit disk at least once before
                // joining the recent queue in.
                cacheRecentOffer(entry);
                faultedToLog.set(true);
                return false;
            }
        } finally {
            synchronizer.unlock();
        }
    }

    /**
     * This method tries to add an entry to pollQueue if possible. There are some scenarios below:
     * 1) If pollQueue starts from scratch and is empty, we add this entry and return TRUE;
     * 2) If pollQueue has previously accepted entries and last accepted LSN seq matches this new entry's previous LSN seq, we add this entry and return TRUE;
     * 3) If this entry has been added into pollQueue, leave without inserting and return TRUE;
     * 4) Otherwise, set faultedToLog flag to TRUE since all log entries have been persisted to log on disk. Finally, quit and return FALSE.
     *
     */
    private boolean acceptOrdered(T entry)
    {
        log.debug("acceptOrdered", "called", "rangeStart", rangeStart, "lastAcceptedSeq", lastAcceptedSeq,
            "entry.startSeq", entry.getRangeStart(), "entry.seq", entry.getRangeEnd());
        if(lastAcceptedSeq == null) {
            if(entry.getRangeStart() == rangeStart) {
                pollQueueSize.addAndGet(sizeDeterminer.getSize(entry));
                pollQueue.add(entry);
                lastAcceptedSeq = entry.getRangeEnd();
                return true;
            } else {
                return false;
            }
        }
        else if (lastAcceptedSeq == entry.getRangeStart()) {
            pollQueueSize.addAndGet(sizeDeterminer.getSize(entry));
            pollQueue.add(entry);
            lastAcceptedSeq = entry.getRangeEnd();
            return true;
        }
        else if (lastAcceptedSeq > entry.getRangeStart()) {
            // No reason to fault to log, we are ahead via the power of magic.
            //  In this situation we can trivially accept the entry, since it has already been added or polled;
            //  i.e., even if it is really old, it is outside the current window over the log.
            return true;
        }
        else { // lastAcceptedSeq.rangeEnd < entry.rangeStart => we have a break in the offer stream.
            return false;
        }
    }

    /**
     * This method tries to add an entry to recentOfferQueue if possible. There are some scenarios below:
     * 1) If recentOfferQueue has sufficient space, we add this entry;
     * 2) If recentOfferQueue does not have sufficient space, we add this entry and evict the oldest entry until recentOfferQueue's size is under limit;
     * 3) If recentOfferQueue has anything wrong (out-of-ordering), we can't cache an entry and simply clear the recentOfferQueue.
     *    It has no impact on data safety since it has been written to log.
     *
     */
    private void cacheRecentOffer(T entry)
    {
        log.debug("cacheRecentOffer", "called", "lastCached.seq", lastCached == null ? null : lastCached.getRangeEnd(), "entry.startseq", entry.getRangeStart(), "entry.seq", entry.getRangeEnd());
        // "lastCached == null" is a fresh case where recentOfferQueue just starts
        // "lastCached.getRangeEnd() == entry.getRangeStart()" just checks the incoming entry LSN's seq and gives an ordering guarantee
        if (lastCached == null || lastCached.getRangeEnd() == entry.getRangeStart()) {
            // put it in the recent offers.
            int entrySize = sizeDeterminer.getSize(entry);
            recentOfferQueue.add(entry);
            lastCached = entry;
            recentQueueSize.addAndGet(entrySize);

            // we try to evict old entries if recentOfferQueue's size is over recentQueueRatio
            // or the entire LBQ'size is over maxSize limit
            // NOTE: We evict the old entries so that the cache always holds the latest logs for a join. 
            // This way, a join is a trivial operation and we can switch to faulted == false once we do the join.
            // Else we will never be sure if there are logs after. 
            while (!recentOfferQueue.isEmpty() &&
                (recentQueueSize.get() > maxSize * recentQueueRatio ||
                recentQueueSize.get() + pollQueueSize.get() > maxSize)) {
                T removed = recentOfferQueue.poll(); //trim the recent queue to size.
                recentQueueSize.addAndGet(-1 * sizeDeterminer.getSize(removed));
                log.debug("cacheRecentOffer", "evicted old entry",
                    "entry.seq", removed.getRangeEnd(),
                    "currentSize", recentQueueSize.get() + pollQueueSize.get(),
                    "entrySize", sizeDeterminer.getSize(removed),
                    "maxSize", maxSize);
            }
        }
        else {
            // cache is broken.  Clean it.
            recentOfferQueue.clear();
            log.info("cacheRecentOffer", "cleared recent cache");
            //fall through to reset the queuesize/lastCached.
        }
        if (recentOfferQueue.isEmpty()) {
            lastCached = null;
            recentQueueSize.set(0);
        }
    }
    
    @Override
    public void close() {
        synchronizer.lock();
        try {
            pollQueue.clear();
            pollQueueSize.set(0);
            lastAcceptedSeq = null;
            recentOfferQueue.clear();
            recentQueueSize.set(0);
            lastCached = null;
            isOpen.set(false);
        } finally {
            synchronizer.unlock();
        }
    }
    
    public boolean isOpen() {
        return isOpen.get();
    }

    /**
     * Updates the internal state when removing an element from the queue.
     */
    private void removed(T item)
    {
        if(item != null) {
            log.debug("removed", "removed item", "item", item);
        }
        synchronizer.lock();
        try {
            if(pollQueue.isEmpty()) {
                pollQueueSize.set(0); // in case of exceptions in the sizeDeterminer callback, this could grow.
            }
            else {
                int removedSize = item == null ? 0 : sizeDeterminer.getSize(item);
                pollQueueSize.addAndGet(-1 * removedSize);
            }
            checkState();
        } finally {
            synchronizer.unlock();
        }
    }

    /**
     * When this data structure is inspected, it should check for progress on reading from the log.
     */
    private void checkState()
    {
        synchronizer.lock();
        try {
            if (faultedToLog.get() &&
                pollQueueSize.get() <= (maxSize - recentQueueSize.get()) * logReadRatio &&
                !readingFromLog.get()) {
                readLogAsync();
            }
        } finally {
            synchronizer.unlock();
        }
    }

    /**
     * If nobody else has done it yet, starts the task to read a buffer from disk.
     */
    private void readLogAsync()
    {
        if(readingFromLog.compareAndSet(false, true)) {
            log.debug("readLogAsync", "starting read log task");
            logReaderExecutor.execute(new ReadLogRunnable());
        }
    }

    // TODO kvc break up log access into small batches.
    /**
     * This Runnable is used to read from the log.  Upon completion of a read, it attempts to join the recent offer queue into the
     *  poll queue.
     */
    private class ReadLogRunnable implements Runnable
    {
        /**
         * This method is called to join the recent queue and the poll queue.  If the ranges of the queues are contiguous
         *  or overlap, the queues may be joined, and further in-memory offers may succeed directly into the poll queue.
         *  
         *  NOTE: Should be called within the synchronizer latch.
         * @return True if the recent queue was drained and joined into the poll queue.
         */
        private boolean joinRecentIntoPoll()
        {
            if(lastAcceptedSeq == null) {
                return false;
            }
            while(!recentOfferQueue.isEmpty() && recentOfferQueue.peek().getRangeStart() < lastAcceptedSeq) {
                recentOfferQueue.poll();
            }
            if(recentOfferQueue.isEmpty() || recentOfferQueue.peek().getRangeStart() == lastAcceptedSeq) {
                drainRecentOffersIntoPollQueue();
                return true;
            } else {
                return false;
            }
        }

        /**
         * Moves the records from recent Queue into poll queue. This doesn't check sizes because
         * the size restriction is across offer and poll queue.
         * 
         *  NOTE: Should be called within the synchronizer latch.
         *  NOTE2: caller should ensure it is safe to do the drain. This function just moves data.
         */
        private void drainRecentOffersIntoPollQueue()
        {
            while(!recentOfferQueue.isEmpty()) {
                T entry = recentOfferQueue.poll();
                int size = sizeDeterminer.getSize(entry);
                pollQueue.offer(entry);
                recentQueueSize.addAndGet(-1 * size);
                pollQueueSize.addAndGet(size);
                lastAcceptedSeq = entry.getRangeEnd();
            }
            lastCached = null;
            recentQueueSize.set(0);
        }

        @Override
        public void run() {
            final String FUNC_NAME = "run";
            try {
                while (maxSize - pollQueueSize.get() - recentQueueSize.get() > 0
                       && faultedToLog.get() && isOpen()) {
                    try {
                        log.debug("run", "Log read started", "lastAcceptedSeq", lastAcceptedSeq);
                        List<T> logs = logReader.readLogs(lastAcceptedSeq);
                        
                        synchronizer.lock();
                        try {
                            for (T entry : logs) {
                                if(!isOpen()){
                                    break;
                                }
                                // logEntries batch sizes are expected to be small, as documented in the interface.
                                // Redundant offers, offers which are at or behind the lastAcceptedSeq, are defined as
                                //  trivial accepts, returning true.  If offers from the memory stream conflict here, we
                                //  simply accept and drop the object.
                                boolean accepted = acceptOrdered(entry);
                                bbAssert(accepted, FUNC_NAME, "Enqueuing operation must be accepted during backfill",
                                        "lastAcceptedSeq", lastAcceptedSeq, "rangeStart", rangeStart, "entryStart", entry.getRangeStart());
                            }
                        } finally {
                            synchronizer.unlock();
                        }
                        
                    }
                    finally {
                        synchronizer.lock();
                        try {
                            if(isOpen()){
                                boolean joined = joinRecentIntoPoll();
                                // if we successfully joined, we empty ALL of recent offer queue into the poll queue. Assert this.
                                bbAssert((joined && recentOfferQueue.isEmpty()) ||
                                       !joined, FUNC_NAME, "Either RecentQueue is empty and join did not finish or join completed with out recent queue being empty");
                                //We have to check with the synchronizer to see if this LSN is still coming from the disk.
                                boolean stillFaulted = synchronizer.isFaulted(lastAcceptedSeq);
                                log.debug(FUNC_NAME, "faultedToLog status after join", "stillFaulted", stillFaulted, "joined", joined);
                                faultedToLog.set(stillFaulted);
                            }
                        } finally {
                            synchronizer.unlock();
                        }
                    }
                }
            }
            finally {
                readingFromLog.set(false);
            }
        }
    }
}