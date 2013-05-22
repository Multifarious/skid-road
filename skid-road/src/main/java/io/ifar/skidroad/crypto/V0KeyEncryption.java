package io.ifar.skidroad.crypto;

import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.util.encoders.Base64;

import java.io.*;

/**
 * Routines for key encryption/encoding and decryption/decoding in deprecated v0 format.
 *
 * Deprecated because it does not include the unencrypted master iv in the result.
 *
 * Format is: <pre>
 *     concat(
 *         base64(
 *             aes_encrypt(
 *                 concat(
 *                     key,iv
 *         ))),
 *         '$',
 *         length(key)
 *     )
 * </pre>
 * @deprecated This scheme encourages masterIV reuse, which is undesirable.
 */
public class V0KeyEncryption {

    /**
     * AES Encrypts and then Base64 encodes a key and initialization vector
     * pair with a master key and iv pair. Deprecated because it does not
     * include the unencrypted master iv in the result.
     *
     * @param key key to be encrypted and encoded
     * @param iv initialization vector to be encrypted and encoded
     * @param masterKey master key with which to perform encryption
     * @param masterIV master initialization vector with which to perform encryption
     * @return Base64 encoded and encrypted representation of key and iv.
     * @see #v0DecodeAndDecryptKey(String, byte[], byte[])
     */
    public static String v0EncryptAndEncodeKey(byte[] key, byte[] iv, byte[] masterKey, byte[] masterIV) {
        ByteArrayOutputStream cryptOut = new ByteArrayOutputStream();

        try (InputStream is = new SequenceInputStream(
                new ByteArrayInputStream(key), new ByteArrayInputStream(iv))) {
            StreamingBouncyCastleAESWithSIC.encrypt(is, cryptOut, masterKey, masterIV);

            StringBuilder result = new StringBuilder(
                    new String(Base64.encode(cryptOut.toByteArray()), StreamingBouncyCastleAESWithSIC.ASCII)
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
     * Reverses {@link #v0EncryptAndEncodeKey(byte[], byte[], byte[], byte[])} operation
     *
     * @param base64AndLength Output from v0EncryptAndEncodeKey
     * @param masterKey master key with which to perform encryption
     * @param masterIV master initialization vector with which to perform encryption
     * @return 2-element array, first element is key and second is iv.
     */
    public static byte[][] v0DecodeAndDecryptKey(String base64AndLength, byte[] masterKey, byte[] masterIV) {
        int suffixAt = base64AndLength.indexOf('$');
        if (suffixAt > 0) {
            return v0DecodeAndDecryptKey(base64AndLength.substring(0, suffixAt), base64AndLength.substring(suffixAt + 1), masterKey, masterIV);
        } else {
            throw new IllegalArgumentException("No '$' delimiter between data and length suffix found.");
        }
    }

    /**
     * Reverses {@link #v0EncryptAndEncodeKey(byte[], byte[], byte[], byte[])}  operation, operating on result of splitting the output on '$'
     *
     * @param encryptedKeyAndIV Base64-encoded, AES-encrypted key and iv payload from first split
     * @param keyLengthStr length of decrypted key from second split
     * @param masterKey master key with which to perform encryption
     * @param masterIV master initialization vector with which to perform encryption
     * @return 2-element array, first element is key and second is iv.
     */
    public static byte[][] v0DecodeAndDecryptKey(String encryptedKeyAndIV, String keyLengthStr, byte[] masterKey, byte[] masterIV) {
        int keyLength = Integer.parseInt(keyLengthStr);
        try (ByteArrayOutputStream decoded = new ByteArrayOutputStream()) {
            Base64.decode(encryptedKeyAndIV, decoded);
            //System.out.println("Decoded " + base64AndLength + " to " + decoded.size() + " bytes: " + toHexString(decoded.toByteArray()) + "; splitting at " + keyLength);
            try (ByteArrayOutputStream decrypted = new ByteArrayOutputStream()) {
                StreamingBouncyCastleAESWithSIC.decrypt(new ByteArrayInputStream(decoded.toByteArray()), decrypted, masterKey, masterIV);
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
