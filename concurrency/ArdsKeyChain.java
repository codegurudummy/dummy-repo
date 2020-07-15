package concurrency;

import amazon.odin.httpquery.model.Material;
import amazon.webservices.platform.auth.secret.OdinSymmetricKeyProvider;
import amazon.webservices.platform.auth.secret.exception.SecretProviderUnavailableException;
import amazon.webservices.platform.auth.security.AbstractSecurityModule;
import amazon.webservices.platform.auth.security.SecurityModule.EncryptionAlgorithm;
import amazon.webservices.platform.auth.security.exception.InvalidCipherException;
import amazon.webservices.platform.auth.security.exception.SecurityModuleUnavailableException;
import amazon.webservices.platform.auth.security.exception.UnknownKeyIdException;

import java.security.InvalidKeyException;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/*
 *  Custom keychain for Ards encryption tasks. It is a wrapper over
 *  AbstractSecurityModule that provides key rotation support through Odin.
 *  Wrapper provides richer Encryption response structure that returns IV used in encryption
 *
 *  @author akshays
 */
public class ArdsKeyChain {

    public static class EncryptResponse {
        public String keyId;
        public byte[] cipher;
        public byte[] iv;

        public EncryptResponse() {
        }

        public EncryptResponse(String keyId, byte[] iv, byte[] cipher) {
            this.keyId = keyId;
            this.iv = iv;
            this.cipher = cipher;
        }
    }

    private static final EncryptionAlgorithm ENCRYPTION_ALGORITHM = EncryptionAlgorithm.AES256_GCM_NoPadding;
    private static final long REFRESH_DEFAULT_MATERIAL_MILLIS = 10 * 60 * 1000;  // 10mins
    private static final Random IV_GEN = new SecureRandom();

    private final String materialSetName_;
    private final OdinSymmetricKeyProvider secretProvider_;
    private final SecurityModuleImpl securityImpl_;
    private final AtomicLong lastRefresh_ = new AtomicLong(0);
    private final AtomicReference<Material> curDefaultMaterial_ = new AtomicReference<>();

    public ArdsKeyChain(String materialSetName) {
        materialSetName_ = materialSetName;
        secretProvider_ = new OdinSymmetricKeyProvider();
        securityImpl_ = new SecurityModuleImpl();
    }

    public EncryptResponse encrypt(byte[] message)
        throws UnknownKeyIdException,
               InvalidKeyException,
               SecurityModuleUnavailableException {
        EncryptResponse resp = new EncryptResponse();

        Material encMaterial = getDefaultMaterial_();
        // generate a random IV;
        resp.iv = new byte[ENCRYPTION_ALGORITHM.keyLength()];
        IV_GEN.nextBytes(resp.iv);

        resp.keyId = Long.toString(encMaterial.getMaterialSerial());
        String reference = secretProvider_.buildReference(materialSetName_, resp.keyId);
        resp.cipher = securityImpl_.encrypt(reference, resp.iv, ENCRYPTION_ALGORITHM, message);

        return resp;
    }

    public byte[] decrypt(EncryptResponse resp)
        throws UnknownKeyIdException,
               InvalidCipherException,
               InvalidKeyException,
               SecurityModuleUnavailableException {

        String reference = secretProvider_.buildReference(materialSetName_, resp.keyId);
        return securityImpl_.decrypt(reference, resp.iv, ENCRYPTION_ALGORITHM, resp.cipher);
    }

    private Material getDefaultMaterial_()
        throws UnknownKeyIdException, SecurityModuleUnavailableException {

        if ((System.currentTimeMillis() - lastRefresh_.get()) >= REFRESH_DEFAULT_MATERIAL_MILLIS) {
            refreshDefaultMaterial_();

        }
        return curDefaultMaterial_.get();
    }

    private synchronized void refreshDefaultMaterial_() throws SecurityModuleUnavailableException {

        // I'm assuming it's better to see if another thread already refreshed things
        // instead of making multiple calls to the OdinDaemon.  This is purely for
        // performance as correctness is enforced by the AtomicReference storing the result.
        if ((System.currentTimeMillis() - lastRefresh_.get()) < REFRESH_DEFAULT_MATERIAL_MILLIS) {
            return;
        }
        try {
            curDefaultMaterial_.set(secretProvider_.getOdinSecret(materialSetName_));
            lastRefresh_.set(System.currentTimeMillis());
        } catch (SecretProviderUnavailableException e) {
            throw new SecurityModuleUnavailableException(e);
        }
    }

    // Implementation that pulls keys from a cache before going to Odin
    private class SecurityModuleImpl extends AbstractSecurityModule {

        private final ConcurrentHashMap<String, byte[]> keyCache_ = new ConcurrentHashMap<>();

        @Override
        protected byte[] getKey(String keyId) throws SecurityModuleUnavailableException {

            // The keyId here should be the full reference (material set name and serial)
            // and the contents of that can't change with Odin.  So cache :-)
            byte[] ret = keyCache_.get(keyId);
            if (ret != null) {
                return ret;
            }
            try {
                ret = secretProvider_.getSecret(keyId);
            } catch (SecretProviderUnavailableException e) {
                throw new SecurityModuleUnavailableException(e);
            }
            keyCache_.put(keyId, ret);
            return ret;
        }

        @Override
        public boolean keyExists(String keyId) {
            // This doesn't look te be used by the AbstractSecurityModule
            // and we don't need it either.
            throw new UnsupportedOperationException();
        }
    }

}