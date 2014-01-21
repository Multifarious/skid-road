package io.ifar.skidroad.streaming;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.crypto.AESInputStream;
import io.ifar.skidroad.crypto.StreamingBouncyCastleAESWithSIC;
import io.ifar.skidroad.awssdk.S3Storage;
import org.bouncycastle.util.encoders.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPInputStream;

/**
 * A utility class for pulling the contents of a {@link LogFile}.
 */
public class StreamingAccess {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingAccess.class);

    private final S3Storage storage;
    private final byte[] masterKey;
    private final byte[] masterIV;

    /**
     * Create a new instance wrapped around the supplied {@link io.ifar.skidroad.awssdk.S3Storage}.
     * @param storage a configured (and started) S3 access instance
     * @param masterKey the master encryption key to use in decrypting files.
     * @param masterIV the master IV (may be null) to use in decrypting files whose key was encoded with the legacy algorithm which does not embed the master IV.
     */
    public StreamingAccess(S3Storage storage, String masterKey, String masterIV) {
        this.storage = storage;
        this.masterKey = Base64.decode(masterKey);
        this.masterIV = masterIV == null ? null : Base64.decode(masterIV);
    }

    /**
     * Obtain a stream for a {@link LogFile}'s contents.
     * @param logFile the log file to download
     * @return the contents of the log file.
     * @throws IOException if one occurs during data handling, either due to network communications or due to uncompressing
     *         and decrypting data.
     */
    public InputStream streamFor(LogFile logFile) throws IOException {
        Preconditions.checkNotNull(logFile);
        Path p;

        try {
            p = storage.get(logFile.getArchiveURI().toString());
        } catch (AmazonServiceException ase) {
            LOG.error("Unable to download log file {} from S3 [ {} / {} ]: ({}) {}",
                    logFile.getID(), ase.getStatusCode(), ase.getErrorCode(), ase.getClass().getSimpleName(),
                    ase.getMessage());
            throw Throwables.propagate(ase);
        } catch (AmazonClientException ace) {
            throw Throwables.propagate(ace);
        }

        byte[][] fileKey = StreamingBouncyCastleAESWithSIC.decodeAndDecryptKey(
                logFile.getArchiveKey(),
                masterKey,
                masterIV
        );

        InputStream is;
        try {
            is = Files.newInputStream(p, StandardOpenOption.DELETE_ON_CLOSE);
            AESInputStream compressedStream = new AESInputStream(is, fileKey[0], fileKey[1]);
            return new GZIPInputStream(compressedStream);
        } catch (IOException ioe) {
            LOG.error("Unable to prepare stream for reading {} from temporary file {}: ({}) {}",
                    logFile.getID(),p,ioe.getClass(), ioe.getMessage());
            throw Throwables.propagate(ioe);
        }
    }

    /**
     * Download the bytes for a {@link LogFile}.
     * @param logFile the log file to download
     * @return the contents of the log file.
     * @throws com.amazonaws.AmazonClientException if one occurs during S3 communications.
     * @throws IOException if one occurs during data handling, either due to network communications or due to uncompressing
     *         and decrypting data.
     */
    public byte[] bytesFor(LogFile logFile) throws AmazonClientException, IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ByteStreams.copy(streamFor(logFile),baos);
        return baos.toByteArray();
    }
}
