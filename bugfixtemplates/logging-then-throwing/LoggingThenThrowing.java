package formatint;

import java.io.File;

import com.github.javaparser.utils.Log;

class FormatIntUsingPercentD {

    void log_warning_then_throw(Foo foo) {
        try {
            foo.invoke();
        } catch (Throwable e) {
            Log.warn(foo.getFailureStatus());
            throw e;
        }
    }
    
    void log_error_then_throw(Foo foo) {
        try {
            foo.invoke();
        } catch (FooException e) {
            Log.error(foo.getFailureStatus());
            throw e;
        }
    }
    
    void log_error_then_throw_custom_exception(Foo foo) {
        try {
            foo.invoke();
        } catch (FooException e) {
            Log.error(foo.getFailureStatus());
            throw new CustomException(e);
        }
    }
    
    void log_error_else_throw(Foo foo) {
        try {
            foo.invoke();
        } catch (FooException e) {
            if (foo.cond()) {
                Log.error(foo.getFailureStatus());
            } else {
                throw e;
            }
        }
    }
    
    void just_log(Foo foo) {
        try {
            foo.invoke();
        } catch (FooException e) {
            Log.error(foo.getFailureStatus());
        }
    }
    
    void just_throw(Foo foo) {
        try {
            foo.invoke();
        } catch (FooException e) {
            throw e;
        }
    }
}
