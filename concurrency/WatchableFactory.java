package concurrency;

import com.a9.cpx.monitoring.pmet.StatusChangeHandler;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class WatchableFactory<T extends Watchable> {

    List<WatchableRegistry> watchableRegistry;
    Map<Attributes,T> watchableMap = new ConcurrentHashMap<Attributes, T>();
    StatusChangeHandler statusChangeHandler;

    public void init() {
    }

    public List<WatchableRegistry> getWatchableRegistry() {
        return watchableRegistry;
    }

    public void setWatchableRegistry(List<WatchableRegistry> watchableRegistry) {
        this.watchableRegistry = watchableRegistry;
    }

    public StatusChangeHandler getStatusChangeHandler() {
        return statusChangeHandler;
    }

    public void setStatusChangeHandler(StatusChangeHandler statusChangeHandler) {
        this.statusChangeHandler = statusChangeHandler;
    }

    protected void register(Watchable watchable) {
        if(watchableRegistry != null) {
            for(WatchableRegistry w : watchableRegistry) {
                w.register(watchable);
            }
        }

        if(statusChangeHandler != null) {
            watchable.setStatusChangeHandler(statusChangeHandler);
        }
    }

    protected boolean unregister(Watchable watchable)  {
        boolean ret = true;
        if(watchableRegistry != null) {
            for(WatchableRegistry w : watchableRegistry) {
                ret&= w.unregister(watchable);
            }
        }
        
        return ret;
    }

    public abstract T newInstance(Attributes attribues);

    public T getWatchable(Attributes attributes) {
        T watchable = watchableMap.get(attributes);
        if(watchable == null) {
            synchronized(this){
                watchable = watchableMap.get(attributes);
                if(watchable == null){
                    watchable = newInstance(attributes);
                    watchableMap.put(attributes, watchable);
                    register(watchable);
                }
            }
        }
        return watchable;
    }

    public void setWatchables(List<T> watchables)  {
        for( Watchable v : watchables) {
            register(v);
        }
    }


    public Collection<T> getWatchables() {
        return watchableMap.values();
    }

}