package bugfixtemplates;

class GettingSubstringBeforeSuffix {

    Object mutex, mutex1;

    public void m1(String className) {
        int i = className.lastIndexOf(SUFFIX);
        className.substring(0, i);
    }

    public void m2(String className) {
        int i = className.lastIndexOf(SUFFIX);
        String subclassPrefix = i > 0 ? className.substring(0, i) : className;
    }

    public void m3(String className) {
        int i = className.indexOf(SUFFIX);
        className.substring(0, i);
    }

    public void m4(String className) {
        int i = className.indexOf(SUFFIX);
        String subclassPrefix = i > 0 ? className.substring(0, i) : className;
    }

    // https://guru-reviews-beta.corp.amazon.com/feedback/internal/CR-11623479/1/
    public String getAttributeKey(String aggregationName) {
        if (aggregationName == null) {
            return null;
        }
        if (aggregationName.endsWith(INTEGER_AGG)) {
            return aggregationName.substring(0, aggregationName.lastIndexOf(INTEGER_AGG));
        } else if (aggregationName.endsWith(LONG_AGG)) {
            return aggregationName.substring(0, aggregationName.lastIndexOf(LONG_AGG));
        } else if (aggregationName.endsWith(STRING_AGG)) {
            return aggregationName.substring(0, aggregationName.lastIndexOf(STRING_AGG));
        }
        return null;
    }

    public void m5(String className) {
        int i = className.indexOf(SUFFIX);
        String subclassPrefix = className.contains(PREFIX) ? className.substring(0, i) : className;
    }

    public void m6(String className) {
        String subclassPrefix = className.contains(SUFFIX) ? className.substring(0, className.indexOf(SUFFIX)) : className;
    }
    
    // https://guru-reviews-beta.corp.amazon.com/feedback/internal/CR-11781566/4/
    public String removeS3Path(final String s) {
        final int index = s.indexOf("s3://");
        if (index >= 0) {
            int c = index + 5;
            while (c < s.length() && !Character.isSpaceChar(s.charAt(c))) {
                ++c;
            }
            if (c < s.length() && s.charAt(c - 1) == '.') {
                --c; // Include the period
            }
            if (c < s.length()) {
                return s.substring(0, index) + "<s3 path>" + s.substring(c, s.length());
            } else {
                return s.substring(0, index) + "<s3 path>";
            }
        }
        return s;
    }
    
    // https://guru-reviews-beta.corp.amazon.com/feedback/internal/CR-11924287/1/
    public String getPath(File child, String root) {
        String path = child.getPath();
        path = path.substring(root.length());
        
        int i = path.lastIndexOf('.');
        if (i == -1) {
            throw new RuntimeException(
                    "Missing file extension on " + child.getPath()
                    );
        }
        
        String extension = path.substring(i);
        if (path.startsWith("/spring/")) {
            if (!extension.equals(".xml")) {
                throw new RuntimeException(
                        "File extension is not .xml on " + child.getPath()
                        );
            }
        } else {
            if (!extension.equals(".ion")) {
                throw new RuntimeException(
                        "File extension is not .ion on " + child.getPath()
                        );
            }
        }

        path = path.substring(0, i);

        return path;
    }
    
    // Caused NoSuchElementException when processing 
    // control nodes without conditions such as try statement nodes.
    public void tryMethod(String s) {
        try {
            s.substring(s.indexOf("PREFIX"));
        } catch (Exception e) {}
    }

    public void m7(String className) {
        int i = className.lastIndexOf(SUFFIX);
        String subclassPrefix = i > 0 ? className.substring(0, i) : className;
        className.substring(0, i);
    }

    public void positiveWithContainsExpression(String className) {
        String subclassPrefix = className.contains(System1.lineSeparator()) ? className.substring(0, className.indexOf(System.lineSeparator())) : className;
    }
    
    public void positiveWithLogicalExpression(String className) {
        int i = className.indexOf(SUFFIX);
        return j != -1 && className.substring(0, i).isEmpty();
    }
    
    public void negativeWithShortCircuiting(String className) {
        int i = className.indexOf(SUFFIX);
        return i != -1 && className.substring(0, i).isEmpty();
    }

    public void negativeWithContainsExpression(String className) {
        String subclassPrefix = className.contains(System.lineSeparator()) ? className.substring(0, className.indexOf(System.lineSeparator())) : className;
    }

}
