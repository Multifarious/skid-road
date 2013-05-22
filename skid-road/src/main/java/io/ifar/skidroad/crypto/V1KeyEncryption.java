package io.ifar.skidroad.crypto;

import com.google.common.collect.Lists;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.util.encoders.Base64;

import java.io.*;
import java.util.List;

import static io.ifar.skidroad.crypto.StreamingBouncyCastleAESWithSIC.*;

/**
 * Created with IntelliJ IDEA.
 * User: lhn
 * Date: 5/22/13
 * Time: 12:31 PM
 * To change this template use File | Settings | File Templates.
 */
public class V1KeyEncryption {
    /**
     * Encrypts provided single-use key and iv pair and returns 7-bit ASCII clean representation of them
     * and and the master iv used for encryption.
     *
     * Intended use is to facilitate key rotation of encrypted artifacts.
     * Suppose artifacts are all encrypted with single-use keys. Those
     * single-use keys are then encrypted with the master key (by this method)
     * and stored.
     * A key rotation can be achieved by decrypting all the single use
     * keys with the old master key and re-encrypting them with a new
     * master key. The encrypted artifacts themselves are untouched.
     *
     * Format is: <pre>
     *     concat(
     *         '1', //format version number
     *         '$',
     *         base64(encrypt(concat(
     *             key,iv
     *         ))),
     *         '$',
     *         length(key),
     *         '$',
     *         base64(master_iv)
     *     )
     * </pre>
     *
     * This format intentionally bears superficial similarity to <a href="http://pythonhosted.org/passlib/modular_crypt_format.html">Modular Crypt Format</a>, but that format represents hashed
     * passwords, not encrypted passwords and initialization vectors.
     *
     * There is a BouncyCastle supported RFC for for encoding passwords and hashes (PKCS5 scheme-2),
     * but it says nothing of encrypting them and including the master IV. There is little to be gained
     * by leveraging it inside of here.
     *
     * @param key Single-use key
     * @param iv Single-use initialization vector
     * @param masterKey Master key used to encrypt the single-use pair. Not included in output.
     * @param masterIV Master initialization vector used to encrypt the single-use pair. Included in output.
     */
    protected static String v1EncryptAndEncodeKey(byte[] key, byte[] iv, byte[] masterKey, byte[] masterIV) {
        ByteArrayOutputStream cryptOut = new ByteArrayOutputStream();

        try (InputStream is = new SequenceInputStream(
                new ByteArrayInputStream(key), new ByteArrayInputStream(iv))) {
            encrypt(is, cryptOut, masterKey, masterIV);
        } catch (InvalidCipherTextException | IOException e) {
            //Unexpected
            throw new IllegalArgumentException("Cannot encrypt provided key and initialization vector", e);
        }
        return JOINER.join(
                '1',
                new String(Base64.encode(cryptOut.toByteArray()), ASCII),
                key.length,
                new String(Base64.encode(masterIV), ASCII)
        );
    }

    /**
     * Reverses {@link #v1EncryptAndEncodeKey(byte[], byte[], byte[], byte[])} operation, operating on result of splitting the output on '$'
     *
     * @param encrypted Output from v1EncryptAndEncodeKey
     * @param masterKey master key with which to perform encryption
     * @return 2-element array, first element is key and second is iv.
     */
    protected static byte[][] v1DecodeAndDecryptKey(String encrypted, byte[] masterKey) {
        List<String> pieces = Lists.newArrayList(SPLITTER.split(encrypted));
        if (pieces.size() != 4) {
            throw new IllegalArgumentException(String.format("Unparsable encrypted key representation '%s'. Expected 4 parts, found %d.", encrypted, pieces.size()));
        }
        if (! "1".equals(pieces.get(0))) {
            throw new IllegalArgumentException(String.format("Unsupported encrypted key scheme '%s'", pieces.get(0)));
        }
        return v1DecodeAndDecryptKey(pieces.get(1), pieces.get(2), pieces.get(3), masterKey);
    }

    /**
     * Reverses {@link #v1EncryptAndEncodeKey(byte[], byte[], byte[], byte[])} operation
     *
     * @param encryptedKeyAndIV Base64-encoded, AES-encrypted key and iv payload from first split
     * @param keyLengthStr length of decrypted key from second split
     * @param encodedMasterIV Base-64 encoded master iv from third split
     * @param masterKey master key with which to perform encryption
     * @return 2-element array, first element is key and second is iv.
     */
    protected static byte[][] v1DecodeAndDecryptKey(String encryptedKeyAndIV, String keyLengthStr, String encodedMasterIV, byte[] masterKey) {
        int keyLength = Integer.parseInt(keyLengthStr);
        byte[] masterIV = Base64.decode(encodedMasterIV);

        try (ByteArrayOutputStream decoded = new ByteArrayOutputStream()) {
            Base64.decode(encryptedKeyAndIV, decoded);
            //System.out.println("Decoded " + encrypted + " to " + decoded.size() + " bytes: " + toHexString(decoded.toByteArray()) + "; splitting at " + keyLength);
            try (ByteArrayOutputStream decrypted = new ByteArrayOutputStream()) {
                decrypt(new ByteArrayInputStream(decoded.toByteArray()), decrypted, masterKey, masterIV);
                //System.out.println("Decrypted to " + decrypted.size() + " bytes: " + toHexString(decrypted.toByteArray()));
                byte[] key = new byte[keyLength];
                byte[] iv = new byte[decrypted.size() - keyLength];
                System.arraycopy(decrypted.toByteArray(),0,key,0,keyLength);
                System.arraycopy(decrypted.toByteArray(),keyLength,iv,0,iv.length);
                return new byte[][] { key, iv };
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Provided String was not Base64 encoded.", e);
        }
    }
}
