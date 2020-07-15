package formatint;

import java.io.File;

import com.github.javaparser.utils.Log;

class FormatIntUsingPercentD {

    void catch_throwable_1(Foo foo) {
        try {
            foo.invoke();
        } catch (Throwable e) {
            Log.error(foo.getFailureStatus());
        }
    }
    
    void catch_throwable_2(Foo foo) {
        try {
            foo.invoke();
        } catch (FooException e) {
            Log.error(foo.getFailureStatus());
        }
    }
}
