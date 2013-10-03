package io.ifar.skidroad.dropwizard.cli;

import com.yammer.dropwizard.cli.Command;
import com.yammer.dropwizard.config.Bootstrap;
import io.ifar.skidroad.crypto.StreamingBouncyCastleAESWithSIC;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.Hex;

/**
 * Utility command to dump random key for log encryption.
 */
public class GenerateRandomKey extends Command {

    public GenerateRandomKey() {
        super("random-key","Randomly generate a new key for use with the encrypted log rolling.");
    }

    @Override
    public void configure(Subparser subparser) {
        // no op
    }

    @Override
    public void run(Bootstrap<?> bootstrap, Namespace namespace) throws Exception {
        byte[] key = StreamingBouncyCastleAESWithSIC.generateRandomKey();
        System.out.println("Key:");
        System.out.println(" Bytes:  " + new String(Hex.encode(key)));
        System.out.println(" Base64: " + new String(Base64.encode(key)));
    }
}
