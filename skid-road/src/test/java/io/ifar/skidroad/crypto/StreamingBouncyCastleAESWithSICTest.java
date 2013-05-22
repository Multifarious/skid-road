package io.ifar.skidroad.crypto;

import org.apache.commons.io.IOUtils;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static io.ifar.skidroad.crypto.StreamingBouncyCastleAESWithSIC.*;
import static io.ifar.skidroad.crypto.V1KeyEncryption.*;
import static org.junit.Assert.*;

public class StreamingBouncyCastleAESWithSICTest {
    private final static Logger LOG = LoggerFactory.getLogger(StreamingBouncyCastleAESWithSICTest.class);

    @Test
    public void testEncryptAndEncodeKey() throws Exception {
        byte[] masterKey = generateRandomKey();
        byte[] key = generateRandomKey();
        byte[] iv = generateRandomKey();
        String enc = encryptAndEncodeKey(key,iv,masterKey);
        byte[][] result = decodeAndDecryptKey(enc, masterKey);
        assertArrayEquals(key, result[0]);
        assertArrayEquals(iv, result[1]);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testV0EncryptAndEncodeKey() throws Exception {
        //Specifically test for bug with FF bytes in the key or iv.
        byte[] masterKey = generateRandomKey();
        byte[] masterIV = generateRandomKey();
        byte[] key = new byte[] {
                (byte)0xDD, (byte)0xFE, (byte)0x99, (byte)0xA8, (byte)0x22, (byte)0x25,
                (byte)0x97, (byte)0x6A, (byte)0xE6, (byte)0xEE, (byte)0x3B, (byte)0xE9,
                (byte)0x3B, (byte)0x66, (byte)0x26, (byte)0x79, (byte)0xDC, (byte)0x79,
                (byte)0x9B, (byte)0xDF, (byte)0xFF, (byte)0xCA, (byte)0x95, (byte)0xE1,
                (byte)0x15, (byte)0x79, (byte)0xBE, (byte)0x2A, (byte)0x04, (byte)0x10,
                (byte)0xC2, (byte)0x68
        };
        byte[] iv = new byte[] {
                (byte)0x2A, (byte)0xD8, (byte)0xE1, (byte)0x3A, (byte)0x60, (byte)0x89,
                (byte)0x5D, (byte)0xC9, (byte)0x5B, (byte)0xEF, (byte)0x68, (byte)0x8C,
                (byte)0xEF, (byte)0xA7, (byte)0x48, (byte)0xFB
        };
        //full path here avoid deprecation warning on import
        String enc = io.ifar.skidroad.crypto.V0KeyEncryption.v0EncryptAndEncodeKey(key, iv, masterKey, masterIV);
        byte[][] result = io.ifar.skidroad.crypto.V0KeyEncryption.v0DecodeAndDecryptKey(enc, masterKey, masterIV);

        //LOG.debug(toHexString(key));
        //LOG.debug(toHexString(iv));
        //LOG.debug(enc);
        //LOG.debug(toHexString(result[0]));
        //LOG.debug(toHexString(result[1]));

        assertArrayEquals(key, result[0]);
        assertArrayEquals(iv, result[1]);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testV1EncryptAndEncodeKey() throws Exception {
        byte[] masterKey = generateRandomKey();
        byte[] masterIV = generateRandomKey();
        //Specifically test for bug with FF bytes in the key or iv.
        byte[] key = new byte[] {
                (byte)0xDD, (byte)0xFE, (byte)0x99, (byte)0xA8, (byte)0x22, (byte)0x25,
                (byte)0x97, (byte)0x6A, (byte)0xE6, (byte)0xEE, (byte)0x3B, (byte)0xE9,
                (byte)0x3B, (byte)0x66, (byte)0x26, (byte)0x79, (byte)0xDC, (byte)0x79,
                (byte)0x9B, (byte)0xDF, (byte)0xFF, (byte)0xCA, (byte)0x95, (byte)0xE1,
                (byte)0x15, (byte)0x79, (byte)0xBE, (byte)0x2A, (byte)0x04, (byte)0x10,
                (byte)0xC2, (byte)0x68
        };
        byte[] iv = new byte[] {
                (byte)0x2A, (byte)0xD8, (byte)0xE1, (byte)0x3A, (byte)0x60, (byte)0x89,
                (byte)0x5D, (byte)0xC9, (byte)0x5B, (byte)0xEF, (byte)0x68, (byte)0x8C,
                (byte)0xEF, (byte)0xA7, (byte)0x48, (byte)0xFB
        };
        String enc = v1EncryptAndEncodeKey(key, iv, masterKey, masterIV);
        byte[][] result = v1DecodeAndDecryptKey(enc, masterKey);
        assertArrayEquals(key, result[0]);
        assertArrayEquals(iv, result[1]);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testFallback() throws Exception {
        byte[] masterKey = generateRandomKey();
        byte[] masterIV = generateRandomKey();
        byte[] key = generateRandomKey();
        byte[] iv = generateRandomKey();
        //full path here avoid deprecation warning on import
        String encV0 = io.ifar.skidroad.crypto.V0KeyEncryption.v0EncryptAndEncodeKey(key, iv, masterKey, masterIV);
        String encV1 = v1EncryptAndEncodeKey(key, iv, masterKey, masterIV);
        byte[][] result = decodeAndDecryptKey(encV1, masterKey, masterIV);
        assertArrayEquals(key, result[0]);
        assertArrayEquals(iv, result[1]);
        result = decodeAndDecryptKey(encV0, masterKey, masterIV);
        assertArrayEquals(key, result[0]);
        assertArrayEquals(iv, result[1]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBadBase64() throws Exception {
        decodeAndDecryptKey("not_base-64", generateRandomKey(), generateRandomIV());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecryptMissingDelimiter() throws Exception {
        byte[] masterKey = generateRandomKey();
        String s = encryptAndEncodeKey(generateRandomKey(), generateRandomIV(), masterKey);
        decodeAndDecryptKey(s.replace('$', '_'), masterKey);
    }

    @Test
    public void testFileRoundTrip() throws Exception {
        String classPathLocation = '/' + StreamingBouncyCastleAESWithSICTest.class.getPackage().getName().replace('.', '/');
        try (InputStream plainInputStream = getClass().getResourceAsStream(classPathLocation + "/plain.txt")) {
            byte[] plainBytes = IOUtils.toByteArray(plainInputStream);
            testBytesRoundTrip(plainBytes);
        }
    }

    @Test
    public void testEmptyRoundTrip() throws Exception {
        testBytesRoundTrip(new byte[0]);
    }

    @Test
    public void testPartialBlockRoundTrip() throws Exception {
        //AES block size is 128 bits = 16 bytes
        byte[] input = new byte[10];
        for (byte i=0; i<input.length; i++)
            input[i] = i;
        testBytesRoundTrip(input);
    }

    @Test
    public void testOneBlockRoundTrip() throws Exception {
        //AES block size is 128 bits = 16 bytes
        byte[] input = new byte[16];
        for (byte i=0; i<input.length; i++)
            input[i] = i;
        testBytesRoundTrip(input);
    }

    @Test
    public void testBlockAndMoreRoundTrip() throws Exception {
        //AES block size is 128 bits = 16 bytes
        byte[] input = new byte[20];
        for (byte i=0; i<input.length; i++)
            input[i] = i;
        testBytesRoundTrip(input);
    }

    @Test
    public void testUniqueKey() throws Exception {
        //Obviously doesn't verify quality of random number generator. But at least ensures one is being used.
        assertArrayNotEquals(generateRandomKey(), generateRandomKey());
    }

    @Test
    public void testUniqueIV() throws Exception {
        //Obviously doesn't verify quality of random number generator. But at least ensures one is being used.
        assertArrayNotEquals(generateRandomIV(), generateRandomIV());
    }

    //too bad jUnit doesn't have assertArrayNotEquals
    private static void assertArrayNotEquals(byte[] a1, byte[] a2) {
        if (a1.length != a2.length)
            return;
        for (int i =0; i<a1.length; i++)
            if (a1[i] != a2[i])
                return;
        fail("arrays are the same");
    }

    private static void testBytesRoundTrip(byte[] plainBytes) throws IOException, InvalidCipherTextException {
        //Using the static encrypt / decrypt methods implicitly tests
        //AESInputStream and AESOutputStream.

        InputStream plainInputStream = new ByteArrayInputStream(plainBytes);
        ByteArrayOutputStream cipherOutputStream = new ByteArrayOutputStream();

        byte[] key = generateRandomKey();
        byte[] iv = generateRandomIV();

        encrypt(plainInputStream, cipherOutputStream, key, iv);

        ByteArrayOutputStream plainOutputStream = new ByteArrayOutputStream();

        decrypt(new ByteArrayInputStream(cipherOutputStream.toByteArray()), plainOutputStream, key, iv);

        assertArrayEquals(plainBytes, plainOutputStream.toByteArray());
    }
}
