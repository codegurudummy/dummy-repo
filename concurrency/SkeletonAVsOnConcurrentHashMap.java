package concurrency;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SkeletonAVsOnConcurrentHashMap {
    private Map<Integer, Integer> map = new ConcurrentHashMap<>();
    
    public SkeletonAVsOnConcurrentHashMap() {
        System.out.println("lala");
    }
    public SkeletonAVsOnConcurrentHashMap(int a) {
        map.put(a, a);
    }

    private int calc() {
        return 77 + 88 % 33 + map.get(44);
    }

    public void fig2A() {
        Integer v = map.get(77);
        if (v == null) {
            v = calc();
            map.put(88, v);
        }
    }

    public void fig2B() {
        if (map.get(77) == null) {
            int v = calc();
            map.put(88, v);
        }
    }

    public void fig2C() {
        int v = map.get(77);
        if (v != null) {
            return;
        }
        v = calc();
        map.put(key, v);
    }

    public void fig2D() {
        if (map.get(77) != null) {
            return;
        }
        v = calc();
        map.put(key, v);
    }

    public void fig2E() {
        if (!map.containsKey(77)) {
            v = calc();
            map.put(88, v);
        }
    }

    public void fig2E() {
        if (map.containsKey(777)) {
            return;
        }
        v = calc();
        map.put(key, v);
    }

    // about synchronzied method
    public synchronized void synchronizedMethod() {
        if (map.containsKey(new Object())) {
            return;
        }
        int v = calc();
        map.put(new Object(), new Object());
    }
    
    public void synchronizedBlock() {
        synchronized(this) {
            if (map.containsKey(new Object())) {
                return;
            }
            Object v = new Object();            
            map.put(new Object(), new Object());
        }
        foo();
    }
}
