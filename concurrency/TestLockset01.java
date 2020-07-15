package concurrency;

import java.util.ArrayList;
import java.util.HashSet;

public class TestLockset01 {
    private ArrayList<Integer> fOne = new ArrayList<>();
    private HashSet<Integer> fTwo = new HashSet<>();
    private Object lockOne = new Object();
    
    
    public TestLockset01(int one) {
        fTwo.remove(77);
    }
    
    public TestLockset01() {
        synchronized (lockOne) {
            fTwo.remove(77);
        }
    }
    
    public void mOne() {
        synchronized (lockOne) {
            fTwo.remove(77);
        }
    }
    
    public void mTwo() {
        synchronized (lockOne) {
            fTwo.add(77);
        }
    }
    
    public synchronized void mTreee() {
//        synchronized (lockOne) 
        {
            fTwo.add(88);
        }
        
    }
}
