package io.ifar.skidroad.prepping;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.crypto.AESOutputStream;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.apache.commons.io.IOUtils;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.util.encoders.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;

import static io.ifar.skidroad.crypto.StreamingBouncyCastleAESWithSIC.*;
import static java.nio.file.StandardOpenOption.*;

/**
 * Not thread-safe.
 *
 * TODO: Test decrypting and decompressing output of this class
 */
public class EncryptAndCompressPrepper extends AbstractPrepWorker {
    private static final Logger LOG = LoggerFactory.getLogger(EncryptAndCompressPrepper.class);

    private final byte[] masterKey;
    private final byte[] masterIV;

    public EncryptAndCompressPrepper(LogFile logFile, LogFileTracker tracker, String masterKeyBase64, String masterIVBase64) {
        super(logFile, tracker);
        masterKey = Base64.decode(masterKeyBase64);
        masterIV = Base64.decode(masterIVBase64);
    }

    @Override
    public Path prepare(Path inputPath) throws PreparationException {
        Path outputPath = withNewExtension(inputPath, ".gz." + DEFAULT_EXTENSION);
        byte[] key = generateRandomKey();
        byte[] iv = generateRandomIV();
        //LOG.debug("Generated archive key and iv {} / {} for {}", StreamingBouncyCastleAESWithSIC.toHexString(key), StreamingBouncyCastleAESWithSIC.toHexString(iv), logFile);
        logFile.setArchiveKey(encryptAndEncodeKeyAndIV(key, iv, masterKey, masterIV));
        //LOG.debug("Archive key encoded: " + logFile.getArchiveKey());
        if (tracker.updateArchiveKey(logFile) != 1)
            throw new PreparationException("Cannot record archive key for " + logFile);

        try (
                InputStream in = Files.newInputStream(inputPath, READ);
                OutputStream fileOut = Files.newOutputStream(outputPath, CREATE, WRITE);
                AESOutputStream aes = new AESOutputStream(fileOut, key, iv);
                GZIPOutputStream gz = new GZIPOutputStream(aes)
        ) {
            int byteCount = IOUtils.copy(in, gz); //buffers internally; no need for Buffered[In|Out]putStream
            gz.finish();
            gz.flush();
            aes.finish();

            LOG.trace("{} bytes read from {}", byteCount, inputPath);
            return outputPath;
        } catch (InvalidCipherTextException | IOException e) {
            throw new PreparationException(String.format("Unable to compress and encrypt %s to %s.", inputPath, outputPath), e);
        }
    }
}
