package concurrency;

import com.amazon.aahydra.experimentplatform.strategies.ExperimentalStrategy;

import java.util.concurrent.ConcurrentHashMap;

/**
 * It is very expensive to search our runtime for default strategies, so we want to cache these results.
 * It is actually expected that it will be incredibly rare for this to change, so we don't even need a ttl
 * cache.
 * @author beldykm
 *
 */
public class DefaultStrategyCache {
    private static ConcurrentHashMap<Class<?>, Class<?>> strategyCache = new ConcurrentHashMap<>();
    
    /**
     * Caches a strategy in the cache.
     * @param strategyInterface the strategy interface
     * @param defaultImplementation the concrete default class
     */
    public static synchronized void encacheStrategy(Class<?> strategyInterface, Class<?> defaultImplementation){
        if(!strategyInterface.isAssignableFrom(defaultImplementation)){
            throw new IllegalArgumentException("Implementation '"+defaultImplementation+"' is not actually a '"+strategyInterface+"'");
        }
        strategyCache.put(strategyInterface, defaultImplementation);
        
    }
    
    /**
     * gets the default strategy class from the cache.
     * @param strategyInterface the strategy interface
     * @param <S> the strategy interface
     * @return the default strategy's class or null if not found.
     */
    @SuppressWarnings("unchecked")
    public static synchronized <S extends ExperimentalStrategy> Class<S> getDefaultImplementationForInterface(Class<S> strategyInterface){
        Class<?> untypedClass = strategyCache.get(strategyInterface);
        if(untypedClass == null){
            return null;
        }
        
        return (Class<S>) strategyCache.get(strategyInterface);
    }
    
    /**
     * Empties out the cache.
     */
    public static synchronized void clearCache(){
        strategyCache = new ConcurrentHashMap<>();
    }

}