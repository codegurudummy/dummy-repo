package concurrency.smallTest;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The MINIMUM reduced messaging.  I.e., the other messaging test has a ton of combinations.  This only the 
 *  minimum necessary to show all coverage
 * */
public class MessagingReducedConcurrentLinkedQueueAVOS01 {
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

    // ================ check size and poll ===============
    
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

}

