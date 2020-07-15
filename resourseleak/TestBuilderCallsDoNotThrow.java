package resourceleak;


import com.amazon.aws.pyramiddb.crypto.encryption.model.EncryptionRequest;
import com.amazon.aws.pyramiddb.crypto.encryption.model.EncryptionResult;
import com.amazon.aws.pyramiddb.crypto.model.EncryptionException;
import com.amazon.aws.pyramiddb.crypto.model.Key;
import com.amazon.mechanicalturk.common.util.Environment;
import com.amazonaws.mechanicalturk.AWSMechanicalTurkRequesterPrivate.LiveTransactionRecord;
import com.mechanicalturk.client.s3.S3Exception;
import lombok.NonNull;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.GeneralSecurityException;

import static java.lang.String.format;


public class TestBuilderCallsDoNotThrow {

    public EncryptionResult testBuilderCallsDoNotThrow(@NonNull final EncryptionRequest encryptionRequest) throws EncryptionException {
        final Key key = encryptionRequest.getKey();

        try {
            final byte[] iv = buildIv();
            final Cipher cipher = buildCipher(iv, key);
            final CipherOutputStream cipherOut = new CipherOutputStream(encryptionRequest.getDataStream(), cipher);

            return EncryptionResult.builder()
                    .dataStream(cipherOut)
                    .nonce(iv)
                    .build();
        } catch (final GeneralSecurityException e) {
            log.error("Failed to encrypt data with key {}", key.getId(), e);
            throw new EncryptionException("Failed to encrypt data.", e);
        } catch (final IllegalArgumentException e) {
            log.error("Key size doesn't match the algorithm expected key size{}", key.getId(), e);
            throw new EncryptionException("Invalid key exception for key id " + key.getId(), e);
        }
    }

    public void testBuilderCallsDoNotThrow2(HttpServletRequest request, HttpServletResponse response) throws ServletException {
        if (Environment.isSandbox()) {
            throw new ServletException("Request is not valid for sandbox environment.");
        }

        if(!response.isCommitted()) {
            response.reset();
        }
        response.setStatus(HttpServletResponse.SC_OK);
        response.setHeader("Cache-Control", "private, max-age=0, s-maxage=0");
        response.setContentType("application/vnd.ms-excel; charset=UTF-8");
        response.setDateHeader("Expires", System.currentTimeMillis());
        response.setHeader("Content-Disposition", "attachment; filename=\"" + clientFileName + "\"");

        boolean errorOccured = true;
        PrintWriter out = null;
        try {
            // write CSV header
            out = response.getWriter();
            out.print(headerRow);

            // Stream the S3 files
            if (s3Files != null) {
                streamS3FilesHAHA(out, s3Files);
            }

            // Stream the live records
            if (liveRecords != null) {
                for (LiveTransactionRecord record: liveRecords) {
                    out.print(record.getTransaction());
                    out.print("\r\n");
                }
            }

            out.flush();
            out.close();

            errorOccured = false;
        } catch(S3Exception e) {
            log.error("S3 error streaming object to user", e);
        } catch(IOException e) {
            log.error("IO error streaming object to user", e);
        } catch(GeneralSecurityException e) {
            log.error("Security error creating decryption input stream", e);
        } catch(RuntimeException e) {
            log.error( format("Runtime error of type %s in %s", e.getClass().getName(), this.getClass().getName() ), e);
        } finally {
            if (errorOccured) {
                out.print(format("%nAn internal error has occured that has prevented the download from completing%n") );
            }
        }
    }
}
