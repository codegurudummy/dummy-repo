package concurrency.smallTest;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This is for testing MESSAGING ONLY
 * */
public class ConcurrentLinkedQueueAtomViolOrOverSync01 {
    private ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();

    String key = "lala" + 33;
    String val = "lala" + 44;

    public String give() {
        queue.remove("key"); // this remove ensures the noRemove() policy does not kick in
        return queue.peek();
    }

    
    //
    //             !!!!!!!!!!!!                         TESTS MESSAGING ONLY !!!!!!!!!!!!!!!!!!!!!!!
    //
    
    // =========== check size and peek ===========

    public void atomViol04() {
        String myVal = queue.peek();
        if (myVal != null) {
            myVal = queue.peek();
            System.out.println(myVal.length()); // ensure accesses null
        }
    }

    public void atomViol06() {
        if (!queue.isEmpty()) {
            String myVal = queue.peek();
            System.out.println(myVal.length()); // ensure accesses null
        }
    }

    public void atomViol07() {
        if (queue.size() > 0) {
            String myVal = queue.peek();
            System.out.println(myVal.length()); // ensure accesses null
        }
    }

    // =========== check object and peek ===========
    // for now, this is not being messaged or detected --- also, NOT likely : to check key and then peek front
    public void atomViol05() {
        if (queue.contains(key)) {
            String myVal = queue.peek();
            System.out.println(myVal.length()); // ensure accesses null
        }
    }


    // ================ check size and poll ===============

    public void atomViol08() {
        if (queue.size() > 0) {
            String myVal = queue.poll();
            System.out.println(myVal.length()); // ensure accesses null
        }
    }
    
    public void atomViol08() {
        if (queue.isEmpty()) {
            String myVal = queue.poll();
            System.out.println(myVal.length()); // ensure accesses null
        }
    }

    // ================ check size and remove ===============
    public void atomViol09() {
        if (queue.size() > 0) {
            String myVal = queue.remove();
            System.out.println(myVal.length()); // ensure accesses null
        }
    }

    public void atomViol10() {
        if (queue.isEmpty()) {
            String myVal = queue.remove();
            System.out.println(myVal.length()); // ensure accesses null
        }
    }
    
    public void atomViol11() {
        if (queue.size() > 0) {
            // should report bug --- because remove can throw exception
            queue.remove();
        }
    }
}

