package io.ifar.skidroad.crypto;

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
import org.bouncycastle.util.encoders.Base64;

import java.io.*;
import java.nio.charset.Charset;
import java.security.SecureRandom;

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
 * @link https://en.wikipedia.org/wiki/Cipher_modes
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
     */    public static PaddedBufferedBlockCipher makeEncryptionCipher(byte[] key, byte[] iv) {
        return makeCipher(ENCRYPT, key, iv);
    }

    /**
     * Generates a Bouncy Castle PaddedBufferedBlockCipher.
     *
     * Generally it is advisable to use AESInputStream, AESOutputStream, or
     * the static encrypt / decrypt methods rather than calling this directly.
     */
    public static PaddedBufferedBlockCipher makeDecryptionCipher(byte[] key, byte[] iv) {
        return makeCipher(DECRYPT, key, iv);
    }

    /**
     * Wraps provided OutputStream in an AESOutputStream and encrypts the
     * provided InputStreap by copying it to the wrapped output.
     *
     * @param from Input data; it is not closed by this method.
     * @param to Output data; it is not flushed or closed by this method.
     * @param key AES encryption key
     * @param key AES SIC initialization vector
     *
     * @see AESOutputStream
     * @throws IOException
     * @throws InvalidCipherTextException
     */
    public static void encrypt(InputStream from, OutputStream to, byte[] key, byte[] iv) throws IOException, InvalidCipherTextException {
        AESOutputStream encryptedOutputStream = new AESOutputStream(to, key, iv);
        IOUtils.copy(from, encryptedOutputStream);
        encryptedOutputStream.finish();
    }

    /**
     * Decrypts the provided InputStream by wrapping it with an AESInputStream
     * and copying it to the provided OutputStream.
     *
     * @param from Input data; it is not closed by this method.
     * @param to Output data; it is not flushed or closed by this method.
     * @param key AES decryption key
     * @param key AES SIC initialization vector
     *
     * @see AESInputStream
     * @throws IOException
     */
    public static void decrypt(InputStream from, OutputStream to, byte[] key, byte[] iv) throws IOException {
        AESInputStream decryptedInputStream = new AESInputStream(from, key, iv);
        IOUtils.copy(decryptedInputStream, to);
    }

    /**
     * Generate a secure random 256-bit key for use with AES. (AES accepts 128, 192, or 256 bit keys)
     */
    public static byte[] generateRandomKey() {
        return keyGenerator.generateKey();
    }

    /**
     * Generate a secure random Initialization Vector (IV) for use with AES block chaining.
     * SIC IVs should never be reused, and this is most easily achieved with a
     * cryptographically secure random number generator.
     *
     * AES IVs are always 128 bits long.
     *
     * @link http://stackoverflow.com/questions/4608489/how-to-pick-an-appropriate-iv-initialization-vector-for-aes-ctr-nopadding?rq=1
     */
    public static byte[] generateRandomIV() {
        return ivGenerator.generateKey();
    }

    /**
     * AES Encrypts and then Base64 encodes a key and initialization vector
     * pair with a master key and iv pair.
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
     *         base64(encrypt(concat(
     *             key,iv
     *         ))),
     *         '$',
     *         length(key)
     *     )
     * </pre>
     *
     * TODO (future): See whether CMSPBEKey or other provides a standard implementation.
     *
     * @param key key to be encrypted and encoded
     * @param iv initialization vector to be encrypted and encoded
     * @param masterKey master key with which to perform encryption
     * @param masterIV master initialization vector with which to perform encryption
     * @return Base64 encoded and encrypted representation of key and iv.
     * @see #decodeAndDecryptKeyAndIV(String, byte[], byte[])
     */
    public static String encryptAndEncodeKeyAndIV(byte[] key, byte[] iv, byte[] masterKey, byte[] masterIV) {
        ByteArrayOutputStream cryptOut = new ByteArrayOutputStream();

        try (InputStream is = new SequenceInputStream(
                new ByteArrayInputStream(key), new ByteArrayInputStream(iv))) {
            encrypt(is, cryptOut, masterKey, masterIV);

            StringBuilder result = new StringBuilder(
                    new String(Base64.encode(cryptOut.toByteArray()), ASCII)
            );
            //System.out.println("Encoded " + toHexString(cryptOut.toByteArray()) + " to " + result + "; appending length " + key.length);
            result.append('$');
            result.append(Integer.toString(key.length));
            return result.toString();
        } catch (InvalidCipherTextException | IOException e) {
            //Unexpected
            throw new IllegalArgumentException("Cannot encrypt provided key and initialization vector", e);
        }
    }

    /**
     * Reverses encryptAndEncodeKeyAndIV operation.
     * @param base64AndLength Output from encryptAndEncodeKeyAndIV
     * @param masterKey master key with which to perform encryption
     * @param masterIV master initialization vector with which to perform encryption
     * @return 2-element array, first element is key and second is iv.
     * @see #encryptAndEncodeKeyAndIV(byte[], byte[], byte[], byte[])
     */
    public static byte[][] decodeAndDecryptKeyAndIV(String base64AndLength, byte[] masterKey, byte[] masterIV) {
        int suffixAt = base64AndLength.indexOf('$');
        if (suffixAt > 0) {
            int keyLength = Integer.parseInt(base64AndLength.substring(suffixAt + 1));
            try (ByteArrayOutputStream decoded = new ByteArrayOutputStream()) {
                Base64.decode(base64AndLength.substring(0,suffixAt), decoded);
                //System.out.println("Decoded " + base64AndLength + " to " + decoded.size() + " bytes: " + toHexString(decoded.toByteArray()) + "; splitting at " + keyLength);
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
        } else {
            throw new IllegalArgumentException("No '$' delimiter between data and length suffix found.");
        }
    }

    public static String toHexString(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 3);
        for (byte b : bytes)
            result.append(String.format("%02X ", b));
        return result.toString().trim();

    }
}
