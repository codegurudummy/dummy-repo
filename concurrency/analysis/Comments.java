package concurrency.analysis;

/**
 * This is for testing messaging, because we have several scenarios that keep interacting one with another
 * */
public class Comments {
    
    /**
     * @param paramOne something about param one
     *              perhaps some more
     * @param paramTwo more about param two --- even more
     * @return perhaps this method returns null
     * @param paramThree trying 
     *          param
     *              three
     * @return second return. 
     *              sure, probably not legit javadoc, but people can write it
     * */
    public int foo1() {
        return 5;
    }
    
    /**
     * Doest this
     * 
     *  thing crash ?
     * @param paramOne something about param one
     *              perhaps some more
     * @param paramTwo more about param two --- even more
     * @return perhaps this method returns null
     * @param paramThree trying 
     *          param
     *              three
     * @return second return. 
     *              sure, probably not legit javadoc, but people can write it
     * */
    public int foo2() {
        return 5;
    }
    
    /**



     * */
    public int foo3() {
        return 5;
    }
    
    //
    //
    //  this should not crash
    //
    //
    public int foo3() {
        return 5;
    }

}
