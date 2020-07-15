package bugfixtemplates;

class CatchSpecificallyThrownException {

    void positiveWithoutExplicitCatch() {
        try {
            foo();
            throw new TestException();
        } catch (IndexOutOfBoundsException e) {
        } catch (Exception e) {
        }
    }

    void negativeWithExplicitCatch() {
        try {
            foo();
            throw new TestException();
        } catch (IndexOutOfBoundsException e) {
        } catch (TestException e) {
        } catch (Exception e) {
        }
    }

    void negativeWithoutTryCatch() {
        foo();
        throw new TestException();
    }

    void positiveWithoutRethrowInABranch() {
        try {
            foo();
            throw new TestException();
        } catch (IndexOutOfBoundsException e) {
        } catch (Exception e) {
            if (bar())
                throw new TestException();
            else
                bar();
        }
    }

    void negativeWithRethrow() {
        try {
            foo();
            throw new TestException();
        } catch (IndexOutOfBoundsException e) {
        } catch (Exception e) {
            bar();
            throw new TestException();
        }
    }

    void negativeWithExplicitCatchInMultiCatch() {
        try {
            foo();
            throw new TestException();
        } catch (IndexOutOfBoundsException e) {
        } catch (Exception | TestException e) {
        }
    }

    void negativeWithRethrowOrHandle() {
        try {
            foo();
            throw new TestException();
        } catch (IndexOutOfBoundsException e) {
        } catch (Exception e) {
            if (bar())
                throw new TestException();
            else
                bar(e);
        }
    }

    void negativeWithStandardException() {
        try {
            foo();
            throw new NullPointerException();
        } catch (IndexOutOfBoundsException e) {
        } catch (Exception e) {
        }
    }
    
    class TestException extends Exception {
        
    }

}
