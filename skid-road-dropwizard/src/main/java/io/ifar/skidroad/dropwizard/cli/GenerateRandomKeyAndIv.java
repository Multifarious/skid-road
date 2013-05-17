package io.ifar.skidroad.dropwizard.cli;

import com.yammer.dropwizard.cli.Command;
import com.yammer.dropwizard.config.Bootstrap;
import io.ifar.skidroad.crypto.StreamingBouncyCastleAESWithSIC;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.Hex;

/**
 * Utility command to dump random key and iv for log encryption.
 */
public class GenerateRandomKeyAndIv extends Command {

    public GenerateRandomKeyAndIv() {
        super("key-and-iv","Create a new key and IV for use with the encrypted log rolling.");
    }

    @Override
    public void configure(Subparser subparser) {
        // no op
    }

    @Override
    public void run(Bootstrap<?> bootstrap, Namespace namespace) throws Exception {
        byte[] key = StreamingBouncyCastleAESWithSIC.generateRandomKey();
        byte[] iv = StreamingBouncyCastleAESWithSIC.generateRandomIV();
        System.out.println("Key:");
        System.out.println(" Bytes:  " + new String(Hex.encode(key)));
        System.out.println(" Base64: " + new String(Base64.encode(key)));
        System.out.println("IV:");
        System.out.println(" Bytes:  " + new String(Hex.encode(iv)));
        System.out.println(" Base64: " + new String(Base64.encode(iv)));
    }
}
