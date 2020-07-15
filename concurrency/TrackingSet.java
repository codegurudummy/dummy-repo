package concurrency;

import amazon.sable.compute.util.diagnostic.DiagnosticInfo;
import amazon.sable.compute.util.diagnostic.DiagnosticInfoProvider;
import amazon.sable.compute.util.diagnostic.impl.RootDiagnosticInfo;
import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.commons.lang.Validate;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * TrackingSet is a thread-safe class which holds three subsets named as eligible, inProgress and stale.
 *
 * <pre>
 *            +-- inProgress
 * eligible --|
 *            +-- stale
 * </pre>
 *
 * It has the same uniqueness guarantees as the Set<E> has. Apart from it, its internal subsets have additional property
 * that an element can only be present in one of subsets at a time.
 *
 * New elements are always added to the eligible subset, and then can be moved to inProgress or slate set, and back to
 * eligible. Adding an element again to this set causes its automatic promotion to eligible, if it was part of stale set
 * earlier.
 *
 */
@ThreadSafe
public class TrackingSet<E> implements DiagnosticInfoProvider {

    @GuardedBy("this")
    private Set<E> eligible = new HashSet<>();

    @GuardedBy("this")
    private Set<E> inProgress = new HashSet<>();

    @GuardedBy("this")
    private Set<E> stale = new HashSet<>();

    /**
     * Adds a new element to eligible set if not contained in any of the subsets, if the element was already present in
     * stale subset it is promoted to eligible.
     *
     * @return true if element was added, false otherwise.
     */
    public synchronized boolean add(E e) {
        if (eligibleOrInProgressContains(e)) {
            return false;
        }
        if (stale.contains(e)) {
            promoteToEligible(e);
            return false;
        }
        return eligible.add(e);
    }

    /**
     * @return true if element is present in either eligible or inProgress subset.
     */
    private boolean eligibleOrInProgressContains(E e) {
        return eligible.contains(e) || inProgress.contains(e);
    }

    /**
     * @return true if element is present in either eligible or inProgress subset.
     */
    public synchronized boolean contains(E e) {
        return eligible.contains(e) || inProgress.contains(e) || stale.contains(e);
    }

    /**
     * @return removes the element from both the sets.
     */
    public synchronized boolean remove(E e) {
        return eligible.remove(e) || inProgress.remove(e) || stale.remove(e);
    }

    /**
     * @return true if both eligible and inProgress subsets are empty.
     */
    public synchronized boolean isEmpty() {
        return eligible.isEmpty() && inProgress.isEmpty() && stale.isEmpty();
    }

    /**
     * @return the size of the inProgress subset.
     */
    public synchronized int size() {
        return eligible.size() + inProgress.size() + stale.size();
    }

    /**
     * @return the size of the eligible subset.
     */
    public synchronized int getEligibleSize() {
        return eligible.size();
    }

    /**
     * @return the size of the inProgress subset.
     */
    public synchronized int getInProgessSize() {
        return inProgress.size();
    }

    /**
     * @return the size of the stale subset.
     */
    public synchronized int getStaleSize() {
        return stale.size();
    }

    /**
     * Moves the element from stale to eligible subset.
     */
    private void promoteToEligible(E e) {
        stale.remove(e);
        eligible.add(e);
    }

    /**
     * Moves the element to inProgress subset if it was already present in eligible.
     */
    public synchronized void moveToInProgress(E e) {
        if (eligible.contains(e)) {
            eligible.remove(e);
            inProgress.add(e);
        }
    }

    /**
     * Moves the element to eligible subset if it was already present in inProgress subset.
     */
    public synchronized void moveToEligible(E e) {
        if (inProgress.contains(e)) {
            inProgress.remove(e);
            eligible.add(e);
        }
    }

    /**
     * Moves the element to stale subset if it was already present in eligible subset.
     */
    public synchronized void moveToStale(E e) {
        if (eligible.contains(e)) {
            eligible.remove(e);
            stale.add(e);
        }
    }

    /**
     * @return a new Set containing all the elements from eligible subset.
     */
    public synchronized Set<E> getEligibleSet() {
        return new HashSet<>(eligible);
    }

    /**
     * Returns a new set containing elements with the given positive size from eligible subset after sorting. The order
     * of the elements may not be retained in the returned set.
     *
     * @param size
     *            positive number of elements to return, if size is greater the current element count in eligible set,
     *            entire set is returned.
     * @param comparator
     *            used to compare the elements in the list to decide ordering.
     *
     * @return the set with the give size after sorting.
     */
    public synchronized Set<E> getSortedEligibleSet(int size, Comparator<E> comparator) {
        Validate.isTrue(size >= 0, "size should be positive integer, but was:" + size);
        if (size >= eligible.size()) {
            return getEligibleSet();
        }
        return new LinkedList<E>(eligible).stream().collect(
                    Collectors.toCollection(
                        // heaping sorting
                        ()->MinMaxPriorityQueue.orderedBy(comparator).maximumSize(size).create()
                    )).stream().collect(Collectors.toSet());
    }

    @Override
    public DiagnosticInfo getDiagnosticInfo() {
        return new SummaryDiagnostic();
    }

    /**
     * Diagnostic information for the tracking set.
     */
    public final class SummaryDiagnostic extends RootDiagnosticInfo {

        public SummaryDiagnostic() {
            addChild("eligble", new SetDiagnosticInfo(eligible));
            addChild("inprogress", new SetDiagnosticInfo(inProgress));
            addChild("stale", new SetDiagnosticInfo(stale));
        }

        public long getEligibleSize() {
            return eligible.size();
        }

        public long getInProgressSize() {
            return inProgress.size();
        }

        public long getStaleSize() {
            return stale.size();
        }

    }

    /**
     * Wrapper to dump out a Set as a child diagnostic so the users aren't overwhelmed with excessive information.
     */
    public final class SetDiagnosticInfo implements DiagnosticInfo {
        private final Set<E> set;

        public SetDiagnosticInfo(Set<E> set) {
            this.set = set;
        }

        public Set<E> getSet() {
            return set;
        }
    }
}