package bugfixtemplates;

class SynchronizedObjectReassignment {

    Object mutex, mutex1;

    void m0() {
        mutex = new Object();
        foo(mutex);
    }

    void m1() {
        synchronized (mutex) {
            mutex = new Object();
            foo(mutex);
        }
    }

    void m2() {
        synchronized (mutex) {
            mutex1 = new Object();
            foo(mutex);
        }
    }

    void m3() {
        synchronized (mutex) {
            mutex = new Object();
            foo(mutex);
        }
        synchronized (mutex) {
            mutex1 = new Object();
            foo(mutex);
        }
        synchronized (mutex1) {
            mutex = new Object();
            foo(mutex1);
        }
    }

    void m4() {
        synchronized (mutex()) {
            mutex1 = new Object();
            foo(mutex());
        }
    }

}
