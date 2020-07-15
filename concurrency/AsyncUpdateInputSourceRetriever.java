package concurrency;

import amazon.platform.config.AppConfig;
import com.amazon.atv.marketplace.Marketplace;
import com.amazon.atv.trending.exception.TrendingProductionException;
import com.amazon.atv.trending.input.InputSource;
import com.amazon.atv.trending.input.parser.Parser;
import com.amazon.atv.trending.retrieval.model.SourceResult;
import com.amazon.atvprecogmetricutils.MetricsUtil;
import com.amazon.atvprecogmetricutils.MetricsUtil.MetricsHelper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.measure.unit.SI;
import java.io.UncheckedIOException;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.amazon.atv.trending.util.DimensionUtil.getDimension;
import static com.amazon.atvprecogmetricutils.Dimensions.dimension;

@Slf4j
public class AsyncUpdateInputSourceRetriever<T> implements Retriever<Marketplace, T> {

    private static final String FAILED_UPDATE = "FailedUpdate";
    private static final String NO_RESULTS = "NoResults";
    private static final String SOURCE_AGE = "SourceAge";
    private static final String UPDATED = "Updated";

    private final MetricsUtil metricsUtil;

    private final Map<Marketplace, InputSource> sources;

    private final Parser<T> parser;

    /**
     * Map accessed across threads. This stores the results for a single input source. All
     * writes to this object are done in synchronized blocks. For safety, since many threads will be
     * reading from here, we will use ConcurrentHashMap
     */
    private final Map<Marketplace, SourceResult<T>> storedResults = new ConcurrentHashMap<>();
    
    public AsyncUpdateInputSourceRetriever(@NonNull final MetricsUtil metricsUtil, 
            @NonNull final Map<Marketplace, InputSource> sources,
            @NonNull final Parser<T> parser,
            @NonNull final ScheduledExecutorService scheduledExecutorService,
            final long updatePeriodMilli) {
        this.metricsUtil = metricsUtil;
        this.sources = sources;
        this.parser = parser;
        scheduledExecutorService.scheduleAtFixedRate(this::backgroundUpdate, 0, updatePeriodMilli, TimeUnit.MILLISECONDS);
    }

    @Override
    public T retrieve(@NonNull final Marketplace marketplace) {
        final InputSource source = sources.get(marketplace);
        Preconditions.checkNotNull(source, "Marketplace, {}, not registered with retriever", marketplace);
        if (storedResults.get(marketplace) == null) {
            // We have not yet captured the result of this input source. We must retrieve it now to return something for this request.
            update(source, marketplace, "UpdateBlocking");
        }

        final SourceResult<T> results = storedResults.get(marketplace);
        if (results == null) {
            final String message = String.format("No results from input source: %s", source.getEasyName());
            log.error(message);
            metricsUtil.addMetrics(getDimension(source), NO_RESULTS, 1);
            throw new TrendingProductionException(message);
        }

        return results.getValue();
    }

    /**
     * Updates the singleton storedResults map. It will retrieve the updated results, and if present, replace the stored value.
     * @param inputSource The input source to retrieve.
     * @param marketplace The marketplace corresponding to the input source.
     */
    private void update(final InputSource inputSource, final Marketplace marketplace, final String updateMetricsInstance) {
        metricsUtil.time(dimension(this.getClass().getSimpleName(), updateMetricsInstance), metrics -> {
            final Optional<Date> modifiedDate = Optional.ofNullable(storedResults.get(marketplace)).map(SourceResult::getModifiedDate);
            final Optional<T> updatedResults = getUpdatedResults(inputSource, modifiedDate, metrics);
            updatedResults.ifPresent(results -> {
                metrics.addCount(UPDATED, 1, getDimension(inputSource));
                final SourceResult inputSourceResult = SourceResult.<T>builder()
                        .modifiedDate(inputSource.getLastModifiedDate())
                        .value(results)
                        .build();
                storedResults.put(marketplace, inputSourceResult);
            });
            if (storedResults.get(marketplace) != null) {
                metrics.addCount(SOURCE_AGE, getStoredResultsAge(storedResults.get(marketplace).getModifiedDate()), getDimension(inputSource));
            }
        });
    }

    /**
     * Actually retrieve and parse from the input source. This is used in both sync and async.
     * @return The list of parsed objects or empty if failure or no update.
     */
    private Optional<T> getUpdatedResults(final InputSource inputSource, final Optional<Date> modifiedDate, final MetricsHelper metrics) {
        try {
            return inputSource.applyToInputStream(modifiedDate, inputStream -> parser.parseInputStream(inputStream));
        } catch (final UncheckedIOException e) {
            log.error("Failed to get input stream from source: {}", inputSource.getInputName(), e);
            metrics.addCount(FAILED_UPDATE, 1, getDimension(inputSource));
            return Optional.empty();
        }
    }

    /**
     * Gets the age of the stored results.
     * @param modifiedDate
     * @return Epoch milliseconds.
     */
    private long getStoredResultsAge(final Date modifiedDate) {
        return System.currentTimeMillis() - modifiedDate.getTime();
    }

    @VisibleForTesting
    public void backgroundUpdate() {
        metricsUtil.withMetrics(metrics -> {
            final long start = System.currentTimeMillis();
            metrics.addProperty("Marketplace", AppConfig.getRealm().name());
            metrics.addProperty("Operation", "AsyncUpdateInputSourceRetriever.backgroundUpdate");
            metrics.addEpochDate("StartTime", start);
            try {
                for (final Map.Entry<Marketplace, InputSource> sourceEntry : sources.entrySet()) {
                    final Marketplace marketplace = sourceEntry.getKey();
                    final InputSource source = sourceEntry.getValue();
                    log.info("Updating input source: {}", source.getEasyName());
                    try {
                        Preconditions.checkNotNull(source, "Marketplace, {}, not registered with retriever", marketplace);
                        update(source, marketplace, "UpdateAsync");
                    } catch (final RuntimeException e) {
                        // Catch all exceptions here. We don't want background update to fail a request.
                        log.error("Failed to update input source: {}", source.getEasyName(), e);
                        metricsUtil.addMetrics("AsyncUpdateFailed", 1);
                    }
                }
            } finally {
                final long end = System.currentTimeMillis();
                metrics.addTime("Time", end - start, SI.MILLI(SI.SECOND));
                metrics.addEpochDate("EndTime", end);
            }
        });
    }

}