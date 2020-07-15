package concurrency;

import amazon.awsc.util.parallel.FoldOp;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * <p>
 * Maintains a set of entries, with a rough measure of age.
 * <p>
 * Provides a fast mechanism to remove any entries older then some number of
 * time cycles. The clocking mechanism is provided externally, so there is no
 * dependency on system time.
 * <p>
 * <p>
 * When creating an AgedSet, a number of timestamps is specified. This number is
 * the number of time cycles to maintain entries before they get removed. Each
 * time put() is called for a particular entry, it refreshes the entry, so only
 * entries that have not been put() recently are dropped.
 * </p>
 * For example:<br>
 * 
 * <pre>
 * AgedSet&lt;T&gt; set = new AgedSet&lt;T&gt;(1);
 * set.put(&quot;hello&quot;);
 * assert(set.contains(&quot;hello&quot;));
 * 
 * set.tick(); // start next time interval
 * set.put(&quot;world&quot;);
 * assert(set.contains(&quot;hello&quot;));
 * assert(set.contains(&quot;world&quot;));
 * 
 * Set&lt;T&gt; dropped = set.tick(); // dropped contains &quot;hello&quot;
 * assert(!set.contains(&quot;hello&quot;));
 * assert(set.contains(&quot;world&quot;));
 * 
 * dropped = set.tick(); // dropped contains &quot;world&quot; 
 * assert(!set.contains(&quot;world&quot;));
 * </pre>
 * 
 * <br>
 * 
 * @author vgough
 * 
 */
public class AgedSet<T>
{
  private static class SetBin<T>
  {
    final private Set<T> set = Collections.synchronizedSet(
        new HashSet<T>());
    int age;

    public SetBin(int startingAge)
    {
      age = startingAge;
    }

    public Set<T> copy()
    {
      synchronized(set)
      {
        return new HashSet<T>(set);
      }
    }
  }

  // historic entries - grouped by age (newest at front of list)
  private final CopyOnWriteArrayList< SetBin<T> > history_;

  // current entries - added since last time cycle began
  private int timestampsToKeep_;
  private int startingAge_ = 0;

  /** Create aged set which keeps the specified number of history entries.
   * Whenever put() is called, the entry is moved to the most current bucket.
   * A call to tick() causes the buckets to be aged and a new 'current' bucket
   * to be added.  The oldest bucket is eventually removed, along with any
   * entries which are still in that bucket. 
   * 
   * @param numTimeStampsToKeep Number of buckets to keep
   */
  public AgedSet(int numTimeStampsToKeep)
  {
    history_ = new CopyOnWriteArrayList< SetBin<T> >();
    makeNewSet();
    timestampsToKeep_ = numTimeStampsToKeep;
  }

  /** For tracking purposes only.  This does not change the internal workings
   * of AgedSet, but simply changes the reported age of a bucket.  The age
   * is increased each time tick() is called.  The starting age is normally 0.
   * 
   * @param age
   */
  public synchronized void setStartingAge(int age)
  {
    startingAge_ = age;
    history_.get(0).age = age;
  }

  /** Returns upper bound on age of replaced item, or 0 if item was not 
   * already in the set.
   * For example, if the item was added since the last call to tick(), then 
   * the upper bound is 1 + starting age.  The starting age is 0 by default.
   */
  public int put(T data)
  {
    boolean put = true;

    // the set didn't already contain the entry.  Remove from old
    // entries to make sure we only have 1 copy in the set.
    // At most there should be one old entry, so if we find it, then
    // we're done.
    for(SetBin<T> set : history_)
    {
      boolean found = put ? !set.set.add(data) : set.set.remove(data);
      if(found)
        return 1 + set.age;
      put = false;
    }

    return 0;
  }

  /** true if the value exists in the set (regardless of age)
   */
  public boolean contains(final T key)
  {
    return isTrue(new BooleanTest<SetBin<T>>(){
      public boolean isTrue(SetBin<T> b) 
      { 
        return b.set.contains(key); 
      }});
  }

  /** true if there are no entries of any age in the set.
   */
  public boolean isEmpty()
  {
    return !isTrue(new BooleanTest<SetBin<T>>(){
      public boolean isTrue(SetBin<T> b) 
      { 
        return !b.set.isEmpty(); 
      }});
  }

  public interface BooleanTest<T>
  {
    boolean isTrue(T val);
  }

  // internal for doing is-true type tests with short circuiting
  private boolean isTrue(BooleanTest<SetBin<T>> test)
  {
    for(SetBin<T> set : history_)
      if(test.isTrue(set))
        return true;

    return false;
  }

  /** Apply an operation to all entries in the set.
   */
  public <OutType> OutType fold(OutType accumulator, 
      final FoldOp<T, OutType> op)
  {
    for(SetBin<T> set : history_)
      for(T v : set.copy())
        accumulator = op.fold(v, accumulator);

    return accumulator;
  }

  /** Add a set of elements to the list.  Same as calling put on each element.
   */
  public void putAll(Collection<? extends T> data)
  {
    for(T t : data)
      put(t);
  }

  /** Returns upper bound on age of item if found.
   * If no item was found, returns < 0
   */
  public int remove(T data)
  {
    // search youngest to oldest..
    for(SetBin<T> set : history_)
      if(set.set.remove(data))
        return 1+set.age;

    return -1;
  }

  /** Remove all entries, no matter what the age.
   *  After this call, isEmpty() is guaranteed to return true.
   */
  public synchronized void clear()
  {
    history_.clear();
    makeNewSet();
  }

  public int numBins()
  {
    return history_.size();
  }

  /**
   * Return a set of entries from the same age.<br>
   * The age can range from 0 to numBins()-1<br>
   * <ul>
   * <li>
   * getBin(0) will return the entries put() since the last call to tick().
   * </li>
   * <li>
   * getBin(1) returns the next oldest entries, etc.
   * </li>
   * </ul>
   */
  public Set<T> getBin(int bin)
  {
    if(bin < history_.size())
      return history_.get(bin).copy();
    else
      return null;
  }

  /** Returns upper bound on age of entry.<br>
   * If the entry has been added since the last call to tick(), then
   * 1+starting age is returned.
   */
  public int getAge(T entry)
  {
    // add 1 to get upper bound (as if tick() might be called any moment)
    for(SetBin<T> set : history_)
      if(set.set.contains(entry))
        return 1+set.age;

    return -1;
  }

  private void makeNewSet()
  {
    history_.add(0, new SetBin<T>(startingAge_));
  }

  /** Age entries.
   * If any entries fall off the end of the queue (the maximum queue length
   * was specified in the constructor), then those entries are returned.
   * 
   * This has the side effect of starting a new time cycle.  All entries
   * inserted after this call up until the next call to cleanup() will be
   * considered to be the same age.
   * 
   * Returns null if there are no entries expired.
   */
  public synchronized Set<T> tick()
  {
    Set<T> res = null;

    // remove oldest entries
    if(history_.size() > timestampsToKeep_)
    {
      SetBin<T> ent = history_.remove(history_.size()-1);
      if(ent != null && !ent.set.isEmpty())
        res = ent.set; // safe to return the set itself
    }

    // update age counter
    for(SetBin<T> set : history_)
      set.age++;

    makeNewSet();

    return res;
  }

  /** Age entries and also change the timestamp history.
   * If the timestamp history is shortened, this may result in more elements
   * being returned then normal.
   * If the timestamp history is lengthened, then no history will be returned.
   */
  public synchronized Set<T> tick(int newTimestampHistory)
  {
    timestampsToKeep_ = newTimestampHistory;
    Set<T> old = tick();

    // if the history was shortened, accumulate all the old entries
    while(history_.size() > timestampsToKeep_+1)
    {
      SetBin<T> ent = history_.remove(history_.size()-1);
      if(!ent.set.isEmpty())
      {
        if(old == null)
          old = ent.copy();
        else
          old.addAll(ent.set);
      }
    }

    return old;
  }
}