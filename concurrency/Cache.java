/*
 * Cache.java
 *
 * Copyright (c) 2010-2011 Amazon Technologies, Inc.  All rights reserved.
 *
 * PROPRIETARY/CONFIDENTIAL
 *
 * Use is subject to license terms.
 */
package concurrency;

import com.amazon.ebook.util.log.Log;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * implements a simple FIFO Cache structure where the data is stored in a Map
 * for quick lookup with the {@link #get(String)} method.
 * <p>
 * using the {@link #put(String, Cacheable)} method on a key that already exists
 * will increment the reference count instead of adding another object into the
 * Cache. the reference count will decrement when {@link Cacheable#close()} is
 * called. objects with multiple reference counts will persist in the Cache,
 * where objects that only have one reference will be removed from the Cache
 * (and will be GCed if {@link Cacheable#close()} was already called on it or
 * the next time that is called on the object).
 */
public class Cache {

    private static final Log LOG = Log.getInstance("Cache");

    private HashMap dataMap;
    private int maxSize;
    private CachedObject firstOut;
    private CachedObject lastIn;

    public Cache(int size) {
        dataMap = new HashMap(size);
        maxSize = size;
        firstOut = null;
        lastIn = null;
    }

    /**
     * returns the Cacheable object associated with the key in the Cache
     * 
     * @param key
     *            String of the key the Cacheable object was stored under
     * @return the Cacheable object associated with the key, or null if that
     *         object did not exist in Cache
     */
    public synchronized Cacheable get(String key) {
        CachedObject data = (CachedObject) dataMap.get(key);
        if (data == null) {
            LOG.error("Attempting to get invalid key from Cache: " + key);
            return null;
        }
        return data.cacheableObj;
    }

    /**
     * Puts the Cacheable object into the the Cache under the key. If there isnt
     * enough room in the cache for another object, the last object is removed
     * from the cache. This is reference counted so multiple of the same object
     * will not be added, but instead will increase the count. objects with
     * multiple references open will not be removed from the Cache if it is too
     * big.
     * 
     * @param key
     *            String of the unique key for the object in the Cache
     * @param value
     *            The Cacheable object to put in the Cache
     */
    public synchronized void put(String key, Cacheable value) {
        CachedObject data = (CachedObject) dataMap.get(key);
        if (data == null) {
            // this object does not currently have anything in the cache
            if (dataMap.size() >= maxSize) {
                removeFirstOut();
            }

            CachedObject newObj = new CachedObject(key, value);

            if (lastIn != null) {
                lastIn.next = newObj;
            }

            if (firstOut == null) {
                firstOut = newObj;
            }

            dataMap.put(key, newObj);
            if( Log.isDebugOn() ) {
                LOG.debug("Inserted new object into Cache: " + key.hashCode());
            }

            lastIn = newObj;

            value.setCached(true);
            value.incrementReferences();
        } else {
            // TODO: for performance, it would be ideal to move this object to
            // the back of the .next list
            data.cacheableObj.incrementReferences();

            // TODO: once we are actually storing these objects in persistant cache, there
            // should not be a time where you try to load a different object over the top of it
            // for now... we do this regularly, so just an "info". upgrade to error later.
            if( data.cacheableObj != value ) {
                LOG.info("Replacing old cache object with new one for key(hashed): " + key.hashCode() );
                //replace the old value with the new one
                data.cacheableObj = value;
            }
            
            if( Log.isDebugOn() ) {
                LOG.debug("Increased reference to object in Cache: " + key.hashCode());
            }
            
            //do some cleanup if needed
            if (dataMap.size() > maxSize) {
                removeFirstOut();
            }
        }
    }
    
//TODO: implement this? it needs to be fixed to modify the .next chain properly    
//    /**
//     * removes a specific object from the Cache. Should probably not be called.
//     * 
//     * @param key
//     *            Specific item to remove
//     */
//    public synchronized void remove(String key) {
//        CachedObject val = (CachedObject) dataMap.remove(key);
//        if (val != null) {
//            val.cacheableObj.setCached(false);
//        } else {
//            LOG.error("Invalid key while attempting to remove object from cache: " + key);
//        }
//    }
    
    /**
     * clears the entire Cache. this should probably not be called, ever (it
     * will mess up reference counting).
     */
    public synchronized void flush() {
        // TODO: it would be faster to just use the .next chain we created here
        Set set = dataMap.entrySet();
        Iterator iterator = set.iterator();
        Map.Entry entry;

        while (iterator.hasNext()) {
            entry = (Map.Entry) iterator.next();
            ((CachedObject) entry.getValue()).cacheableObj.setCached(false);
        }

        dataMap.clear();
    }
    
    /**
     * FOR UNIT TESTS ONLY. this just makes some internal data public for ease
     * of use.
     * 
     * @return the current size of the Cache (number of entries in the map)
     */
    public synchronized int size() {
        return dataMap.size();
    }
    
    /**
     * pretty print method for debugging
     * <p>
     * {@inheritDoc}
     */
    public String toString() {
        StringBuffer sb = new StringBuffer("[");
        Iterator i = dataMap.keySet().iterator();
        while(i.hasNext()) {
            sb.append(i.next());
            sb.append(',');
        }
        sb.append(']');
        return sb.toString();
    }

    /**
     * removes the first object from the cache that does not have multiple references
     */
    private void removeFirstOut() {
        CachedObject otherObj = null;
        CachedObject toRemove = firstOut;
        while (toRemove != null && toRemove.cacheableObj.getReferenceCount() > 1) {
            otherObj = toRemove;
            toRemove = toRemove.next;
        }
        if (toRemove != null) {
            if (otherObj != null) {
                // if we aren't removing the head node
                otherObj.next = toRemove.next;
            } else {
                // update the head node
                firstOut = firstOut.next;
            }
            otherObj = (CachedObject) dataMap.remove(toRemove.key);
            toRemove.cacheableObj.setCached(false);
        } else {
            LOG.error("Unable to remove any object from Cache as they all have multiple references");
        }
    }
}

/**
 * internal class to track each node in the order they come in
 */
class CachedObject {
    String key;
    Cacheable cacheableObj;
    CachedObject next;
    
    public CachedObject(String key, Cacheable obj) {
        if( key == null || obj == null ) {
            throw new InvalidParameterException("key and obj values cannot be null for CachedObject");
        }
        this.key = key;
        cacheableObj = obj;
        next = null;
    }
}