package bugfixtemplates;

import java.util.Collection;
import java.util.stream.Stream;

class CheckAnyMatchUsingFindFirst {
    
    Collection<String> col;

    void positiveStreamSortThenOnlyFindFirst() {
        col.stream().sorted(null).findFirst();
    }

    void negativeSortThenDoNotFindFirst() {
        Stream<String> sorted = col.stream().sorted(null);
        sorted.collect(null);
    }

    void positiveSortExistingStreamThenFindFirst(Stream<String> stream) {
        stream.sorted(null).findFirst();
    }

    void negativeStreamSortThenFindFirstButAlsoNeedsTheSorted() {
        Stream<String> sorted = col.stream().sorted(null);
        sorted.findFirst();
        sorted.collect(null);
    }

}
