package bugfixtemplates;

import java.io.File;

class CheckFileDeletionResult {

    void positive1(File file) {
        if (file.exists()) {
            file.delete();
        }
    }

    void negativeIf(File file) {
        if (file.exists()) {
            if (!file.delete()) {
                throw new RuntimeException("Cannot write uploaded file to disk!");
            }
        }
    }

    void positive2(File file) {
        if (file.exists()) {
            file.delete();
            if (file.exists()) {
                return;
            }
        }
    }

    void negativeWhile(File file) {
        boolean deleted = false;
        while (!deleted) {
            try {
                deleted = file.delete();
            } catch (Exception e) {
            }
        }
        log("Deleted=" + deleted);
    }

    void negativeUseEvenNotChecked(File file) {
        log("Deleted=" + file.delete());
    }

}
