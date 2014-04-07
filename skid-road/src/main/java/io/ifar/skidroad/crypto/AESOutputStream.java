package io.ifar.skidroad.crypto;

import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Wraps AES encryption around an OutputStream. Cipher implementation provided
  * by Bouncy Castle via StreamingBouncyCastleAESWithSIC shim.
 *
 * Intended as a easy-to-use best-practice shim on top of what Bouncy Castle
 * provides. StreamingBouncyCastleAESWithSIC documentation describes
 * cryptography settings.
 *
 * @see StreamingBouncyCastleAESWithSIC
 */
public class AESOutputStream extends FilterOutputStream {
    private final PaddedBufferedBlockCipher cipher;
    private final byte[] outputBuffer;
    //long totalOutputByteCount;
    //ByteArrayOutputStream debug = new ByteArrayOutputStream();

    public AESOutputStream(final OutputStream out, final byte[] key, final byte[] iv) {
        super(out);
        //totalOutputByteCount = 0;
        //System.out.println("Writing with key " + StreamingBouncyCastleAESWithSIC.toHexString(key));
        //System.out.println("Writing with iv " + StreamingBouncyCastleAESWithSIC.toHexString(iv));
        this.cipher = StreamingBouncyCastleAESWithSIC.makeEncryptionCipher(key, iv);
        //doFinal can generate up to 2 blocks worth of data. getOutputSize for a single byte of input is a bit superfluous.
        this.outputBuffer = new byte[Math.max(cipher.getBlockSize() * 2, cipher.getOutputSize(1))];
    }

    /**
     * Encrypts the provided input byte. Ciphertext is written to underlying
     * OutputStream if block boundary has been reached.
     * @param input a byte to write
     * @throws IOException if one occurs on the underlying stream
     */
    @Override
    public void write(int input) throws IOException {
        //Even though write takes an int, only the lower byte contains data.

        int outputByteCount = cipher.processByte((byte) input, outputBuffer, 0);
        if (outputByteCount > 0) {
            super.out.write(outputBuffer, 0, outputByteCount);
            //totalOutputByteCount += outputByteCount;
            //debug.write(outputBuffer, 0, outputByteCount);
        }
    }

    /**
     * Finishes writing encrypted data to the output stream without closing
     * the underlying stream. Use this method when applying multiple filters
     * in succession to the same output stream.
     * @throws IOException if an I/O error has occurred
     * @throws org.bouncycastle.crypto.InvalidCipherTextException if the cipher text is invalid.
     */
    public void finish() throws IOException, InvalidCipherTextException {
        int outputByteCount = cipher.doFinal(outputBuffer, 0);
        if (outputByteCount > 0) {
            //System.out.println("finish() generated " + outputByteCount + " more cipher bytes.");
            super.out.write(outputBuffer, 0, outputByteCount);
            //debug.write(outputBuffer, 0, outputByteCount);
        //} else {
        //    System.out.println("finish() generated no additional cipher bytes.");
        }
        //totalOutputByteCount += outputByteCount;
        //System.out.println("Total cipher bytes written: " + totalOutputByteCount);
        //System.out.println(StreamingBouncyCastleAESWithSIC.toHexString(debug.toByteArray()));
    }
}
