package io.ifar.skidroad.streaming;

import com.google.common.io.ByteStreams;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.crypto.AESInputStream;
import io.ifar.skidroad.crypto.StreamingBouncyCastleAESWithSIC;
import io.ifar.skidroad.jets3t.JetS3tStorage;
import org.bouncycastle.util.encoders.Base64;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.StorageObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * A utility class for pulling the contents of a {@link LogFile}.
 */
public class StreamingAccess {

    private final JetS3tStorage storage;
    private final byte[] masterKey;
    private final byte[] masterIV;

    /**
     * Create a new instance wrapped around the supplied {@link JetS3tStorage}.
     * @param storage a configured (and started) S3 access instance
     * @param masterKey the master encryption key to use in decrypting files.
     * @param masterIV the master IV to use in decrypting files.
     */
    public StreamingAccess(JetS3tStorage storage, String masterKey, String masterIV) {
        this.storage = storage;
        this.masterKey = Base64.decode(masterKey);
        this.masterIV = Base64.decode(masterIV);
    }

    /**
     * Obtain a stream for a {@link LogFile}'s contents.
     * @param logFile the log file to download
     * @return the contents of the log file.
     * @throws ServiceException if one occurs during S3 communications.
     * @throws IOException if one occurs during data handling, either due to network communications or due to uncompressing
     *         and decrypting data.
     */
    public InputStream streamFor(LogFile logFile) throws ServiceException, IOException {

        byte[][] fileKey = StreamingBouncyCastleAESWithSIC.decodeAndDecryptKeyAndIV(
                logFile.getArchiveKey(),
                masterKey,
                masterIV
        );

        StorageObject so;
        try {
            so = storage.get(logFile.getArchiveURI().toString());
        } catch (ServiceException e) {
            throw new RuntimeException("Cannot fetch " + logFile.getArchiveURI().toString() + ": " + e.getClass().getSimpleName() + ": " + e.toString());
        }

        InputStream encryptedCompressedStream = so.getDataInputStream();
        AESInputStream compressedStream = new AESInputStream(encryptedCompressedStream, fileKey[0], fileKey[1]);
        return  new GZIPInputStream(compressedStream);
    }

    /**
     * Download the bytes for a {@link LogFile}.
     * @param logFile the log file to download
     * @return the contents of the log file.
     * @throws ServiceException if one occurs during S3 communications.
     * @throws IOException if one occurs during data handling, either due to network communications or due to uncompressing
     *         and decrypting data.
     */
    public byte[] bytesFor(LogFile logFile) throws ServiceException, IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ByteStreams.copy(streamFor(logFile),baos);
        return baos.toByteArray();
    }
}
