package concurrency.smallTest;

import java.util.concurrent.ConcurrentHashMap;

/**
 * This is for testing messaging, because we have several scenarios that keep interacting one with another
 * */
public class SmallAtomViorOrOverSync {
    private ConcurrentHashMap<String, String> myMap = new ConcurrentHashMap<>();

    String key = "lala" + 33;
    String val = "lala" + 44;
    
    public String give() {
        myMap.remove("key"); // this remove ensures the noRemove() policy does not kick in
        return myMap.get("lala");
    }
    
    // ========= check and put =========
    public void atomViol01() {
        String myVal = myMap.get(key);
        if (myVal == null) {
            myMap.put(key, val);
        }
    }
    
    public void atomViol02() {
        if (myMap.get(key) == null) {
            myMap.put(key, val);
        }
    }
    
    public void atomViol03() {
        if ( ! myMap.containsKey(key)) {
            myMap.put(key, val);
        }
    }
    
    // =========== check and get ===========
    
    public void atomViol04() {
        String myVal = myMap.get(key);
        if (myVal != null) {
            String myVal = myMap.get(key);
            System.out.println(myVal.length()); // ensure accesses null
        }
    }
    
    public void atomViol05() {
        if (myMap.get(key) != null) {
            String myVal = myMap.get(key);
            System.out.println(myVal.length()); // ensure accesses null
        }
    }
    
    public void atomViol06() {
        if ( myMap.containsKey(key)) {
            String myVal = myMap.get(key);
            System.out.println(myVal.length()); // ensure accesses null
        }
    }
    
    // ================ check and remove ===============
    
    public void atomViol_checkRemove_01() {
        if (myMap.get(key) != null) {
            String myVal = myMap.remove(key);
            System.out.println(myVal.size());            
        }
    }
    
    // ========= check and put with additional info about the put =========
    public void atomViol07() {
        String myVal = myMap.get(key);
        if (myVal == null) {
            myMap.put(key, val);
        }
        return val;
    }
    
    public void atomViol08() {
        if (myMap.get(key) == null) {
            myMap.put(key, val);
        }
        System.out.println(val);
    }
    
    public void atomViol09() {
        if ( ! myMap.containsKey(key)) {
            myMap.put(key, val);
        }
        System.out.println(val);
        return val;
    }
    
    // ================== over sync with put ================
 
    public void atomViol07() {
        String myKey = "lala" + 88;
        String sillyVal = "lala" + 99;
        synchronized(this) {
            String localVal = myMap.get(myKey);
            if (localVal == null) {
                myMap.put(myKey, sillyVal);
            }
            return myKey;
        }
    }
    
    public synchronized void atomViol08() {
        String myKey = "lala" + 88;
        String sillyVal = "lala" + 99;
        if (myMap.get(myKey) == null) {
            myMap.put(myKey, sillyVal);
        }
    }
    
    public String atomViol09() {
        String myKey = "lala" + 88;
        String sillyVal = "lala" + 99;
        synchronized (this) {
            if ( ! myMap.containsKey(myKey)) {
                myMap.put(myKey, sillyVal);
            }
        }
        System.out.println(sillyVal);
        return sillyVal;
    }
    public String atomViol10() {
        String myKey = "lala" + 88;
        String sillyVal = "lala" + 99;
        synchronized (this) {
            boolean contains = myMap.containsKey(myKey);
            if ( ! contains) {
                myMap.put(myKey, sillyVal);
            }
        }
        System.out.println(sillyVal);
        return sillyVal;
    }
    
    // =================== over sync with remove =============
    
    public void overSync11() {
        String myKey = "lala" + 88;
        String sillyVal = "lala" + 99;
        synchronized (this) {
            boolean contains = myMap.containsKey(myKey);
            if ( ! contains) {
                myMap.remove(myKey);
            }
        }
    }
    
    // ========= check and putIfAbsent second param =========
    public String atomViol01() {
        final String myVal = myMap.get(key);
        if (myVal == null) {
            myMap.putIfAbsent(key, val);
        }
        return val;
    }

    public String atomViol02() {
        if (myMap.get(key) == null) {
            myMap.putIfAbsent(key, val);
        }
        return val;
    }

    public String atomViol03() {
        if (!myMap.containsKey(key)) {
            myMap.putIfAbsent(key, val);
        }
        return val;
    }
}













