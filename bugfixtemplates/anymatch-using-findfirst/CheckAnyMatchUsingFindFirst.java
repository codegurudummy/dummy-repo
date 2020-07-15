package bugfixtemplates;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

class CheckAnyMatchUsingFindFirst {
    
    Collection<String> col;

    void positiveFilterThenFindFirstAndOnlyCheckPresent() {
        col.stream().filter(null).findFirst().isPresent();
    }

    void negativeFilterThenFindFirstButAlsoNeedsTheMatch() {
        Optional<String> any = col.stream().filter(null).findFirst();
        if (any.isPresent())
            any.get();
    }

    void positiveFilterThenFindAnyAndOnlyCheckPresent() {
        col.stream().filter(null).findAny().isPresent();
    }

    void negativeFilterThenFindAnyButAlsoNeedsTheMatch() {
        Optional<String> any = col.stream().filter(null).findAny();
        if (any.isPresent())
            any.get();
    }

}
