package io.ifar.skidroad.prepping;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.apache.commons.io.IOUtils;
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
 * TODO: Unit test decompressing output of this class
 */
public class CompressPrepper extends AbstractPrepWorker {
    private static final Logger LOG = LoggerFactory.getLogger(CompressPrepper.class);

    public CompressPrepper(LogFile logFile, LogFileTracker tracker) {
        super(logFile, tracker);
    }

    @Override
    public Path prepare(Path inputPath) throws PreparationException {
        Path outputPath = withNewExtension(inputPath, ".gz." + DEFAULT_EXTENSION);
        if (tracker.updateArchiveKey(logFile) != 1)
            throw new PreparationException("Cannot record archive key for " + logFile);

        try (
                InputStream in = Files.newInputStream(inputPath, READ);
                OutputStream fileOut = Files.newOutputStream(outputPath, CREATE, WRITE);
                GZIPOutputStream gz = new GZIPOutputStream(fileOut)
        ) {
            int byteCount = IOUtils.copy(in, gz); //buffers internally; no need for Buffered[In|Out]putStream
            gz.finish();
            gz.flush();

            LOG.trace("{} bytes read from {}", byteCount, inputPath);
            return outputPath;
        } catch (IOException e) {
            throw new PreparationException(String.format("Unable to compress %s to %s.", inputPath, outputPath), e);
        }
    }
}
