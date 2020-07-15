package bugfixtemplates;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

class WithoutTimezoneSimpleDateFormatCreation {

    void m1() {
        new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    }

    void m2() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        df.format(new Date());
    }

    void m3() {
        new SimpleDateFormat("yyyy-MM-dd'Z'").format(new Date());
    }

    void m4() {
        new SimpleDateFormat("yyyy-MM-dd'z'").format(new Date());
    }

    void m5() {
        new SimpleDateFormat("yyyy-MM-dd'X'").format(new Date());
    }

    // https://guru-reviews-beta.corp.amazon.com/feedback/internal/CR-11661276/2
    public static String getValidDateFromAndOwningClientName(final Date validDateFrom, final String clientName) {
        final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        return dateFormat.format(validDateFrom) + DataKeyModelConstants.DELIMITER + clientName;
    }

}
