package concurrency.smallTest;

import java.util.concurrent.ConcurrentHashMap;

/**
 * This is putIfAbsent() second parameter tests
 * */
public class SmallAtomViorOrOverSync03 {
    private ConcurrentHashMap<String, String> myMap = new ConcurrentHashMap<>();

    String key = "lala" + 33;
    String val = "lala" + 44;
    
    public String give() {
        myMap.remove("key"); // this remove ensures the noRemove() policy does not kick in
        return myMap.get("lala");
    }
    
    // BUG
    public String f01(String id) {
        String someKey = "cat" + 88;
        if (myMap.containsKey(someKey)) {
            myMap.putIfAbsent(id, someKey);
        }
        return someKey; // uses without checks
    }
    
    // PRUNE
    public String f02(String id) {
        String someKey = "cat" + 88;
        if (myMap.containsKey(someKey)) {
            myMap.putIfAbsent(id, someKey); // soneKey is not used
        }
        return "lala"; 
    }
    
    // PRUNE
    public String f03(String id) {
        String someKey = "cat" + 88;
        if (myMap.containsKey(someKey)) {
            String existingVal = myMap.putIfAbsent(id, someKey);
            if(existingVal == null) {
                existingVal = someKey; // typical pattern of using putIfAbsent.  we should not report this
            }
        }
        return existingVal;
    }
    
    // PRUNE 
    //  TODO: right now right now we do not report this (hence PRUNE), but in the long run we should
    public String f04(String id) {
        String someKey = "cat" + 88;
        if (myMap.containsKey(someKey)) {
            String existingVal = myMap.putIfAbsent(id, someKey);
            if(existingVal == null) {
                existingVal = someKey; // typical pattern of using putIfAbsent.  we should not report this
            }
        }
        return someKey; // however, they use the value that may not have been put in the map
    }
    
    // PRUNE 
    //  do NOT consider putIfAbsent() as guard -- for now --- later on we can look more into it
    public String f05(String id) {
        String someKey = "cat" + 88;
        
        // do NOT report putIfAbsent() as guard --- for get
        if (myMap.putIfAbsent(id, someKey) != null) {
            String existingVal = myMap.remove(someKey);
            return existingVal;
        }
        return someKey; // however, they use the value that may not have been put in the map
    }
}
