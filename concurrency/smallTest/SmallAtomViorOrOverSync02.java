package concurrency.smallTest;

import java.util.concurrent.ConcurrentHashMap;

/**
 * This is for testing corner cases without having a full file that triggers them.
 * */
public class SmallAtomViorOrOverSync02 {
    private ConcurrentHashMap<String, String> myMap = new ConcurrentHashMap<>();

    String key = "lala" + 33;
    String val = "lala" + 44;
    
    public String give() {
        myMap.remove("key"); // this remove ensures the noRemove() policy does not kick in
        return myMap.get("lala");
    }
    
    // tests FP filtering: if the method may return *N*U*L*L*, then get() may return *N*U*L*L*, and therefore you 
    //  should not report it
    //          duringAVReturnsNullButUsingThisNullIsLegal
    public String f01(String id) {
        String someKey = "cat" + 88;
        if (myMap.containsKey(someKey)) {
            return myMap.get(id);
        } else {
            return null; // should be filtered out because of this
        }
    }
    
    // this should report bug.  There is not proof that this method may legaly return *N*U*L*L*
    public String f02(String id) {
        String someKey = "cat" + 88;
        if (myMap.containsKey(someKey)) {
            return myMap.get(id);
        }
        return "lala";
    }
    
    // should filter FP: the comments tells us the method may return *N*U*L*L*
    /**
     * @return value null for whatever reason
     * */
    public String f03(String id) {
        String someKey = "cat" + 88;
        if (myMap.containsKey(someKey)) {
            return myMap.get(id);
        }
        return "lala";
    }
    // this SHOULD report bug.  because the *N*U*L*L* is not in the return tag
    //  if this gets filtered as FP, it means we are WRONGLY interpreting the *N*U*L*L* outside the @return tag
    //      as a *N*U*L*L*  inside the return tag
    /**
     * something something null more something
     * @return something something
     * */

    public String f04(String id) {
        String someKey = "cat" + 88;
        if (myMap.containsKey(someKey)) {
            return myMap.get(id);
        }
        return "lala";
    }

}
