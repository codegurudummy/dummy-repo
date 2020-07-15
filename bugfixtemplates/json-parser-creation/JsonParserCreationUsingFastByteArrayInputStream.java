package bugfixtemplates;

class JsonParserCreationUsingFastByteArrayInputStream {

    Object mutex, mutex1;

    void m1() {
        new JsonFactory().createJsonParser(new FastByteArrayInputStream(t.getBytes()));
    }

    void m2() {
        new JsonFactory().createJsonParser(new FastByteArrayInputStream(t.getBytes(), t.getLength()));
    }

    void m3() {
        new JsonFactory().createJsonParser(new FastByteArrayInputStream(t.getBytes(), t1.getLength()));
    }

    void m4() {
        new JsonFactory().createJsonParser(new FastByteArrayInputStream(t.getBytes(), t.getLength()));
        new JsonFactory().createJsonParser(new FastByteArrayInputStream(t.getBytes(), t.getLength()));
    }

    void m5() {
        new JsonFactory().createJsonParser(new FastByteArrayInputStream(t.getBytes(), t.getLength()));
        new JsonFactory().createJsonParser(new FastByteArrayInputStream(t.getBytes()));
    }

    void m6() {
        new JsonFactory().createJsonParser(new FastByteArrayInputStream(t.getBytes()));
        new JsonFactory().createJsonParser(new FastByteArrayInputStream(t.getBytes()));
    }

}