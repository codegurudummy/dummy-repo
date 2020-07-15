package concurrency;

import alf.easyclient.AlfWatchHandler;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import swami.types.AlfPath;
import swami.types.AlfRegistryElement;
import swami.types.AlfUpdate;

import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * CacheHolder is an abstraction that stores entries for 
 * caching, and handle how to update the cache, and notify 
 * listener if any changes
 * 
 * @author longuyen
 */
class CacheHolder<T> implements AlfWatchHandler {
    
    /**
     * Immutable structure to wraps around an update event
     */
    private static class UpdateEvent {
        final long alfTime;
        final List<AlfUpdate> updates;
        
        UpdateEvent(final long alfTime, final List<AlfUpdate> updates) {
            this.alfTime = alfTime;
            this.updates = updates;
        }
    }
    //////////////////////END OF NESTED CLASS UpdateEvent ///////////////////////////  
    
    // queue for coming update
    private final Queue<UpdateEvent> queue = new ConcurrentLinkedQueue<UpdateEvent>();
    private final AlfPath directory;
    
    // cache client's listener
    private final CopyOnWriteArrayList<AsyncDirCacheListener<T>> listeners
            = new CopyOnWriteArrayList<AsyncDirCacheListener<T>>();
    
    // actual cache
    protected final Cache<AlfPath, Supplier<T>> cache;
    protected final Function<? extends AlfRegistryElement, T> transform;
            
    // to make sure initialize() invoked only once
    private final AtomicBoolean isInitialized = new AtomicBoolean();
    
    // isReady indicates that pathToElement is initialized properly
    private volatile boolean isReady = false;
    
    <E extends AlfRegistryElement> CacheHolder(
            final Cache<AlfPath, Supplier<T>> cache, 
            final AlfPath dirPath,
            final Function<E,T> transform) {            
        this.cache = cache;
        this.directory = dirPath;
        this.transform = transform;
    }
    
    @Override
    public void onWatchedUpdates(long alfTime, List<AlfUpdate> updates) {
        queue.add(new UpdateEvent(alfTime, updates));
        if (isReady)
            processQueue();            
    }
    
    /**
     * initialize the cache with a collection of elements, executed only once
     */
    void initialize(final Collection<? extends AlfRegistryElement> elements) {
        if (isInitialized.getAndSet(true))
            return;            
        
        for (AlfRegistryElement element : elements) {
            if (element == null)
                continue;
            cache.put(element.path, transformLazilyAndMemoize(element, transform));
        }

        isReady = true;
        processQueue();
    }
    
    private synchronized void processQueue() {
        UpdateEvent event;
        while ((event = queue.poll()) != null) {                
            process(event);
        }
    }
    
    /**
     * process an update event
     */
    private void process(final UpdateEvent event) {
        if (event.updates.isEmpty())
            return;
                
        // update the cache
        for (final AlfUpdate update : event.updates) {
            update(update.newValue);
        }
        
        notify(event);
    }
    
    /**
     * Update cache with an element only if it is 
     * a child of the directory path
     */
    private void update(final AlfRegistryElement e) {
        if (!e.path.isChildOf(directory))
            return;
        
        if (e.isNull())
            cache.invalidate(e.path);
        else
            cache.put(e.path, transformLazilyAndMemoize(e, transform));
    }
    
    Supplier<T> transform(final AlfRegistryElement e) {
        return transformLazilyAndMemoize(e, transform);
    }
    
    @SuppressWarnings("unchecked")
    private <E extends AlfRegistryElement> Supplier<T> transformLazilyAndMemoize (
            final AlfRegistryElement e,
            final Function<E, T> function) {
        return Suppliers.memoize( Suppliers.compose(function, Suppliers.ofInstance( (E)e)));
    }    
    
    boolean addListener(final AsyncDirCacheListener<T> listener) {
        return listeners.add(listener);        
    }
    
    boolean removeListener(final AsyncDirCacheListener<T> listener) {
        return listeners.remove(listener);
    }
    
    /**
     * Notify client's listener
     */
    private void notify(final UpdateEvent event) {
        for (final AsyncDirCacheListener<T> listener : listeners) {
            listener.onUpdate(event.alfTime, event.updates);
        }
    }
}