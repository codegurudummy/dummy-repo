package concurrency.smallTest;

import java.util.concurrent.ConcurrentHashMap;

/**
 * The MINIMUM reduced messaging.  I.e., the other messaging test has a ton of combinations.  This only the 
 *  minimum necessary to show all coverage
 * */
public class MessagingReducedConcurrentHashMapAVOS01 {
    private ConcurrentHashMap<String, String> myMap = new ConcurrentHashMap<>();

    String key = "lala" + 33;
    String val = "lala" + 44;
    
    public String give() {
        myMap.remove("key"); // this remove ensures the noRemove() policy does not kick in
        return myMap.get("lala");
    }
    
    // =========== check and get ===========
    
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
    
    // ================== over sync with put ================
    
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

    public String atomViol03() {
        if (!myMap.containsKey(key)) {
            myMap.putIfAbsent(key, val);
        }
        return val;
    }
}













