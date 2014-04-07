package io.ifar.skidroad.crypto;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.bouncycastle.crypto.BlockCipher;
import org.bouncycastle.crypto.CipherKeyGenerator;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.KeyGenerationParameters;
import org.bouncycastle.crypto.engines.AESFastEngine;
import org.bouncycastle.crypto.modes.SICBlockCipher;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.params.ParametersWithIV;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.List;

/**
 * Provides encryption/decryption of InputStream to OutputStream using Bouncy
 * Castle and AES in Segmented Integer Counter (aka SIC or CTR) mode. Intended
 * as a easy-to-use best-practice shim on top of what Bouncy Castle provides.
 *
 * AES is the NIST-approved replacement for DES (and also known as Rijndael
 * with 128-bit block size).  Other block ciphers, such as Serpent or Twofish,
 * would be fine too.
 *
 * SIC is one of the block chaining modes recommended by Niels Ferguson and
 * Bruce Schneier. Others would do, but SIC is parallelizable. (It is
 * parallelizable because the initialization vector for block n is the original
 * IV plus n. In contrast, chaining modes such as CBC use the encrypted output
 * from block n-1 to generate the IV for block n.) The implementation here,
 * however, is single-threaded.
 *
 * Helper methods to generate keys and initialization vectors using Java's
 * SecureRandom are provided.
 *
 * See <a href="https://en.wikipedia.org/wiki/Cipher_modes">Wikipedia</a> for more
 * on cipher modes.
 */
public class StreamingBouncyCastleAESWithSIC {
    /**
     * AES accepts 128. 192, and 256 bit keys.
     */
    public static final int AES_KEY_SIZE_BITS = 256;

    /**
     * AES initialization vectors are always 128 bits (which is the same as the block size).
     */
    public static final int AES_IV_SIZE_BITS = 128;

    public static final String DEFAULT_EXTENSION = "aes-sic";

    public static final Charset ASCII = Charset.forName("US-ASCII");
    public static Joiner JOINER = Joiner.on('$');
    public static Splitter SPLITTER = Splitter.on('$');

    private static final boolean ENCRYPT = true;
    private static final boolean DECRYPT = false;

//    private final byte[] inputBuffer;
//    private final BlockCipher cipherWithSICWrapping;
    private final static CipherKeyGenerator keyGenerator = new CipherKeyGenerator();
    private final static CipherKeyGenerator ivGenerator = new CipherKeyGenerator();
    static {
        keyGenerator.init(new KeyGenerationParameters(new SecureRandom(), AES_KEY_SIZE_BITS));
        ivGenerator.init(new KeyGenerationParameters(new SecureRandom(), AES_IV_SIZE_BITS));
    }

    /**
     * Generates a Bouncy Castle PaddedBufferedBlockCipher.
     *
     * Generally it is advisable to use AESInputStream, AESOutputStream, or
     * the static encrypt / decrypt methods rather than calling this directly.
     * @param encrypt {@code true} for encrypt, {@code false} for decrypt.
     * @param key AES encryption key
     * @param iv AES SIC initialization vector. Should be unique for each invocation.
     * @return a cipher instance
     */
    public static PaddedBufferedBlockCipher makeCipher(boolean encrypt, byte[] key, byte[] iv) {
        //AESFastEngine uses a few KB extra RAM to contain static lookup tables
        //of data which the other implementations need to compute on the fly.
        BlockCipher underlyingAESCipher = new AESFastEngine();
        BlockCipher cipherWithSICWrapping = new SICBlockCipher(underlyingAESCipher);
        PaddedBufferedBlockCipher cipher = new PaddedBufferedBlockCipher(cipherWithSICWrapping);
        cipher.init(encrypt, new ParametersWithIV(new KeyParameter(key), iv));
        return cipher;
    }

    /**
     * Generates a Bouncy Castle PaddedBufferedBlockCipher.
     *
     * Generally it is advisable to use AESInputStream, AESOutputStream, or
     * the static encrypt / decrypt methods rather than calling this directly.
     *
     * @param key AES encryption key
     * @param iv AES SIC initialization vector. Should be unique for each invocation.
     * @return a cipher instance set for encryption
     */
    public static PaddedBufferedBlockCipher makeEncryptionCipher(byte[] key, byte[] iv) {
        return makeCipher(ENCRYPT, key, iv);
    }

    /**
     * Generates a Bouncy Castle PaddedBufferedBlockCipher.
     *
     * Generally it is advisable to use AESInputStream, AESOutputStream, or
     * the static encrypt / decrypt methods rather than calling this directly.
     * @param key AES encryption key
     * @param iv AES SIC initialization vector. Should be unique for each invocation.
     * @return a cipher instance set for decryption.
     */
    public static PaddedBufferedBlockCipher makeDecryptionCipher(byte[] key, byte[] iv) {
        return makeCipher(DECRYPT, key, iv);
    }

    /**
     * Wraps provided OutputStream in an AESOutputStream and encrypts the
     * provided InputStream by copying it to the wrapped output.
     *
     * Usually it is preferable to use the encrypt method that auto-generates the IV.
     *
     * @param from Input data; it is not closed by this method.
     * @param to Output data; it is not flushed or closed by this method.
     * @param key AES encryption key
     * @param iv AES SIC initialization vector. Should be unique for each invocation.
     *
     * @see AESOutputStream
     * @throws IOException if one occurs on the underlying stream
     * @throws InvalidCipherTextException if one occurs during ecryption
     */
    public static void encrypt(InputStream from, OutputStream to, byte[] key, byte[] iv) throws IOException, InvalidCipherTextException {
        AESOutputStream encryptedOutputStream = new AESOutputStream(to, key, iv);
        IOUtils.copy(from, encryptedOutputStream);
        encryptedOutputStream.finish();
    }

    /**
     * Wraps provided OutputStream in an AESOutputStream and encrypts the
     * provided InputStream by copying it to the wrapped output.
     *
     * An AES SIC initialization vector is automatically generated and
     * returned. It it required to decrypt the output!
     *
     * @param from Input data; it is not closed by this method.
     * @param to Output data; it is not flushed or closed by this method.
     * @param key AES encryption key
     * @return randomly generated AES SIC initialization vector
     *
     * @see AESOutputStream
     * @throws IOException it one occurs during underlying operations.
     * @throws InvalidCipherTextException if one occurs during encryption.
     */
    public static byte[] encrypt(InputStream from, OutputStream to, byte[] key) throws IOException, InvalidCipherTextException {
        byte[] iv = generateRandomIV();
        encrypt(from, to, key, iv);
        return iv;
    }

    /**
     * Decrypts the provided InputStream by wrapping it with an AESInputStream
     * and copying it to the provided OutputStream.
     *
     * @param from Input data; it is not closed by this method.
     * @param to Output data; it is not flushed or closed by this method.
     * @param key AES decryption key
     * @param iv AES SIC initialization vector
     *
     * @see AESInputStream
     * @throws IOException if one occurs during underlying operations.
     */
    public static void decrypt(InputStream from, OutputStream to, byte[] key, byte[] iv) throws IOException {
        AESInputStream decryptedInputStream = new AESInputStream(from, key, iv);
        IOUtils.copy(decryptedInputStream, to);
    }

    /**
     * @return a secure random 256-bit key for use with AES. (AES accepts 128, 192, or 256 bit keys)
     */
    public static byte[] generateRandomKey() {
        return keyGenerator.generateKey();
    }

    /**
     * Generate a secure random Initialization Vector (IV) for use with AES block chaining.
     * SIC IVs should never be reused, and this is most easily achieved with a
     * cryptographically secure random number generator.
     *
     * AES IVs are always 128 bits long.  See <a href="http://stackoverflow.com/questions/4608489/how-to-pick-an-appropriate-iv-initialization-vector-for-aes-ctr-nopadding?rq=1">this SO discussion</a>
     * about picking IVs.
     *
     * @return randomly generated initialization vector
     *
     */
    public static byte[] generateRandomIV() {
        return ivGenerator.generateKey();
    }

    /**
     * Returns a 7-bit ASCII clean representation of the provided key/iv.
     * The key and iv themselves are encrypted using the master key and a
     * randomly generated master iv. The master iv included (plaintext) in
     * the encoded result.
     *
     * Intended use is to facilitate key rotation of encrypted artifacts.
     * Suppose artifacts are all encrypted with single-use keys. Those
     * single-use keys are then encrypted with the master key (by this method)
     * and stored.
     * A key rotation can be achieved by decrypting all the single use
     * keys with the old master key and re-encrypting them with a new
     * master key. The encrypted artifacts themselves are untouched.
     *
     * This implementation generates output in the v1 format.
     *
     * @param key to be encrypted and encoded
     * @param iv to be encrypted and encoded
     * @param masterKey to be used during encryption
     *
     * @return ASCII-encoded key
     */
    public static String encryptAndEncodeKey(byte[] key, byte[] iv, byte[] masterKey) {
        return V1KeyEncryption.v1EncryptAndEncodeKey(key, iv, masterKey, generateRandomIV());
    }

    /**
     * Reverses an {@link #encryptAndEncodeKey(byte[], byte[], byte[])} operation.
     *
     * Supports v1 format.
     *
     * @param encryptedAndEncodedKey to be decoded and decrypted
     * @param masterKey to be used during decryption
     * @return 2-element array, first element is key and second is iv. The master iv is not returned.
     */
    public static byte[][] decodeAndDecryptKey(String encryptedAndEncodedKey, byte[] masterKey) {
       return decodeAndDecryptKey(encryptedAndEncodedKey, masterKey, null);
    }

    /**
     * Reverses an {@link #encryptAndEncodeKey(byte[], byte[], byte[])} operation.
     *
     * Supports v0 through v1 formats.
     *
     * @param encryptedAndEncodedKey to be decoded and decrypted
     * @param masterKey to be used during decryption
     * @param masterIV to be used during decryption for the legacy v0 format which does not encode the masterIV
     * @return 2-element array, first element is key and second is iv. The master iv is not returned.
     */
    public static byte[][] decodeAndDecryptKey(String encryptedAndEncodedKey, byte[] masterKey, byte[] masterIV) {
        List<String> pieces = Lists.newArrayList(SPLITTER.split(encryptedAndEncodedKey));
        switch (pieces.size()) {
            case 1:
                throw new IllegalArgumentException(String.format("Unrecognized key encryption format. No '$' delimiter found in '%s'.", encryptedAndEncodedKey));
            case 2:
                //This is the legacy format in which a fixed master IV was used rather than embedding a dynamically generated one
                if (masterIV == null) {
                    throw new IllegalArgumentException("Legacy key encryption format found; master IV must be supplied in order to decrypt.");
                } else {
                    //Legacy decryption algorithm
                    return v0DecodeAndDecryptKey(pieces.get(0), pieces.get(1), masterKey, masterIV);
                }
            case 4:
                //This is the modern format.
                String version = pieces.get(0);
                if ("1".equals(version)) {
                    return V1KeyEncryption.v1DecodeAndDecryptKey(pieces.get(1), pieces.get(2), pieces.get(3), masterKey);
                } else {
                    throw new IllegalArgumentException(String.format("Unknown encryption format version '%s' in '%s'", version, encryptedAndEncodedKey));
                }
            default:
                throw new IllegalArgumentException(String.format("Unrecognized key encryption format. Expected 2 or 4 '$' delimiters in '%s'.", encryptedAndEncodedKey));

        }
    }

    /**
     * shim to selectively suppress deprecation warnings
     */
    @SuppressWarnings("deprecation")
    private static byte[][] v0DecodeAndDecryptKey(String encryptedKeyAndIV, String keyLengthStr, byte[] masterKey, byte[] masterIV) {
        return V0KeyEncryption.v0DecodeAndDecryptKey(encryptedKeyAndIV, keyLengthStr, masterKey, masterIV);
    }

    public static String toHexString(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 3);
        for (byte b : bytes)
            result.append(String.format("%02X ", b));
        return result.toString().trim();

    }
}
