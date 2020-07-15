package concurrency;

import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.*;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Iterates through the given iterator and runs the given function for each element in parallel and returns the results again as iterator. This way
 * processing of each element in the first iterator can be parallelized.
 *
 * @param <S>
 *            type of the input iterator.
 * @param <T>
 *            type of the output iterator.
 * @author celikelm
 */
@Slf4j
@Builder
public class ParallelTransformingIterable<S, T> implements Iterable<T> {
    @NonNull
    private final Iterable<S> source;
    @NonNull
    private final Function<S, T> transformer;
    @NonNull
    private final ExecutorService executor;
    @NonNull
    private final Integer parallelism;

    @Override
    public Iterator<T> iterator() {
        return new ParallelTransformingIterator(source, transformer, executor, parallelism);
    }

    /*
     * Main iterator that initializes the multi-segment iterables and maintains the blocking queue.
     */
    private final class ParallelTransformingIterator extends AbstractIterator<T> {
        private static final int POLL_TIMEOUT_MILLIS = 10;
        private static final int PROGRESS_MILESTONE = 1000;

        private final Iterable<S> source;
        private final Function<S, T> transformer;
        private final Integer parallelism;

        private final ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<T>();
        private final ListeningExecutorService listeningExecutor;
        private final Iterator<List<S>> sourceIt;

        private final AtomicBoolean done = new AtomicBoolean(false);
        private final AtomicInteger transformationFailures = new AtomicInteger(0);
        private final AtomicInteger inflightTransformations = new AtomicInteger(0);
        private final AtomicLong iteratedFromSource = new AtomicLong(0);

        /**
         * Private to ParallelTransformingIterable which can not pass null variables.
         * 
         * @param source
         * @param transformer
         * @param executor
         * @param parallelism
         */
        ParallelTransformingIterator(final Iterable<S> source, final Function<S, T> transformer, final ExecutorService executor,
                final Integer parallelism) {
            this.source = source;
            this.transformer = transformer;
            this.listeningExecutor = MoreExecutors.listeningDecorator(executor);
            this.parallelism = parallelism;
            this.sourceIt = Iterables.partition(source, parallelism).iterator();
        }

        @Override
        protected T computeNext() {
            if (transformationFailures.get() > 0) {
                throw new RuntimeException("Parallel iteration failed. Some source elements could not be transformed");
            }

            if (inflightTransformations.get() < parallelism) {
                submitNextBatch();
            }

            if (inflightTransformations.get() == 0 && queue.size() == 0) {
                log.info("Iterated from source {} ", iteratedFromSource.get());
                return endOfData();
            }

            while (queue.size() == 0) {
                try {
                    Thread.sleep(POLL_TIMEOUT_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            return queue.poll();

        }

        private void submitNextBatch() {
            if (sourceIt.hasNext()) {
                for (S val : sourceIt.next()) {
                    logProgress();
                    inflightTransformations.incrementAndGet();
                    ListenableFuture<T> future = listeningExecutor.submit(() -> transformer.apply(val));
                    Futures.addCallback(future, new FutureCallback<T>() {
                        @Override
                        public void onFailure(Throwable throwable) {
                            inflightTransformations.decrementAndGet();
                            transformationFailures.incrementAndGet();
                            log.error("Transformation failed", throwable);
                        }

                        @Override
                        public void onSuccess(T t) {
                            queue.add(t);
                            inflightTransformations.decrementAndGet();
                        }
                    });
                }
            }
        }

        private void logProgress() {
            iteratedFromSource.incrementAndGet();
            if (0 == iteratedFromSource.get() % PROGRESS_MILESTONE) {
                log.info("Iterated from source {} ", iteratedFromSource.get());
            }
        }
    }
}