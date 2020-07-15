package formatint;

import java.io.File;

class FormatIntUsingPercentD {

    int getFileLengthShim(File file) {
        return (int) file.length();
    }
    
    void formatint_m11(File file) {
        int i = file.length();
        String s = String.format("File length is %s", i);
        LOG.info(s);
    }
    
    void formatint_m12(File file) {
        String s = String.format("File length is %s", getFileLengthShim(file) + 1);
        LOG.info(s);
    }

    void formatint_m2(File file) {
        int i = file.length();
        String s = String.format("File length is %d", i);
        LOG.info(s);
    }
    
    void formatint_m3(File file) {
        int i = file.length();
        String s = String.format("File length is %2d", i);
        LOG.info(s);
    }
    
    void formatint_m4(File file) {
        int i = file.length();
        String s = format("File length is {}", i);
        LOG.info(s);
    }
    
    public static RubyHash fieldAccess(Object... args) {
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException(String.format("Odd number of arguments: %s (%s)", args.length, StringUtils.join(args, ", ")));
        }
        RubyHash hash = new RubyHash(RUBY);
        for (int index = 0; index < args.length; index += 2) {
        }
    }
}
