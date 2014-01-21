package io.ifar.skidroad.crypto;

import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wraps AES decryption around an InputStream. Cipher implementation provided
 * by Bouncy Castle via StreamingBouncyCastleAESWithSIC shim.
 *
 * Intended as a easy-to-use best-practice shim on top of what Bouncy Castle
 * provides. StreamingBouncyCastleAESWithSIC documentation describes
 * cryptography settings.
 *
 * @see StreamingBouncyCastleAESWithSIC
 */
public class AESInputStream extends InputStream {
    private final InputStream in;
    private final PaddedBufferedBlockCipher cipher;
    private final byte[] outputBuffer;
    int outputBufferOffset = 0;
    int outputBufferEnd = 0;
    boolean inputStreamIsDone = false;
    //long totalInputByteCount;
    //ByteArrayOutputStream debug = new ByteArrayOutputStream();

    public AESInputStream(final InputStream in, final byte[] key, final byte[] iv) {
        this.in = in;
        //System.out.println("Reading with key " + StreamingBouncyCastleAESWithSIC.toHexString(key));
        //System.out.println("Reading with iv " + StreamingBouncyCastleAESWithSIC.toHexString(iv));
        //totalInputByteCount = 0;
        this.cipher = StreamingBouncyCastleAESWithSIC.makeDecryptionCipher(key, iv);
        this.outputBuffer = new byte[cipher.getOutputSize(1)];
    }

    private int getNextByteFromBuffer() throws IOException {
        int remainingBytes = outputBufferEnd - outputBufferOffset;
        if (remainingBytes == 0)
            return -1;
        else {
            int result = outputBuffer[outputBufferOffset++];
            if (outputBufferOffset == outputBufferEnd)
                outputBufferOffset = outputBufferEnd = 0; //indicates nothing left in buffer
            //convert byte to read() 0-255 int format
            return result < 0 ? result + 256 : result;
        }
    }

    /**
     * Returns the next plaintext byte. May or may not invoke read() on the
     * underlying InputStream.
     * @throws IOException
     */
    @Override
    public int read() throws IOException {
        if (outputBufferEnd > 0)
            return getNextByteFromBuffer(); //we have bytes in the buffer from last time. Return those.

        if (inputStreamIsDone)
            return -1; //nothing left in the buffer nor in the input stream. We're done.

        while (true) {
            int maybeByte = in.read();
            if (maybeByte < 0) {
                //System.out.println("Total cipher bytes read: " + totalInputByteCount);
                //System.out.println(StreamingBouncyCastleAESWithSIC.toHexString(debug.toByteArray()));
                inputStreamIsDone = true;
                try {
                    outputBufferEnd = cipher.doFinal(outputBuffer, 0);
                } catch (InvalidCipherTextException e) {
                    throw new IOException(e);
                }
                return getNextByteFromBuffer();

            } else {
                //convert input.read() from 0-255 int into a byte
                byte b = (byte) (maybeByte < 0 ? maybeByte + 256 : maybeByte);
                //totalInputByteCount++;
                //debug.write(b);
                outputBufferEnd = cipher.processByte(b, outputBuffer, 0);
                if (outputBufferEnd > 0) {
                    return getNextByteFromBuffer();
                }
                //else haven't yet read a full 128 bit AES block. Keep going.
            }
        }
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
