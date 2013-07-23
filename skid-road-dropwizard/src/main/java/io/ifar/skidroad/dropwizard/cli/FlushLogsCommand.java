package io.ifar.skidroad.dropwizard.cli;

import com.google.common.collect.ImmutableSet;
import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.config.Environment;
import io.ifar.goodies.CliConveniences;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.dropwizard.ManagedPrepWorkerManager;
import io.ifar.skidroad.dropwizard.ManagedUploadWorkerManager;
import io.ifar.skidroad.tracking.LogFileState;
import io.ifar.skidroad.tracking.LogFileStateListener;
import io.ifar.skidroad.tracking.LogFileTracker;
import io.ifar.skidroad.writing.WritingWorkerManager;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import static io.ifar.skidroad.tracking.LogFileState.*;

/**
 * Prepares and uploads any in-progress log files on this host. Service must not be currently running! Used to
 * facilitate decommissioning a host without leaving behind any data.
 */
@SuppressWarnings("UnusedDeclaration")
public abstract class FlushLogsCommand<T extends Configuration> extends ConfiguredCommand<T>
{
    private final static Logger LOG = LoggerFactory.getLogger(FlushLogsCommand.class);

    public FlushLogsCommand() {
        super("flush-logs","Prepare and upload any in-progress log files on this host. SERVICE MUST NOT BE RUNNING.");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);
    }

    /**
     * Subclasses should overwrite and create singleton instances returned by the other abstract methods. Manager objects
     * created should be registered with the Environment for proper lifecycle handling.
     * @param configuration
     * @param environment
     */
    protected abstract void init(T configuration, Environment environment) throws Exception;
    protected abstract LogFileTracker getLogTracker();
    protected abstract WritingWorkerManager getWritingWorkerManager();
    protected abstract ManagedPrepWorkerManager getPrepWorkerManager();
    protected abstract ManagedUploadWorkerManager getUploadWorkerManager();


    @Override
    protected void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {

        Environment env = CliConveniences.fabricateEnvironment(getName(), configuration);
        init(configuration,env);

        LogFileTracker logFileTracker = getLogTracker();
        //Get a WritingWorkerManager because, on start-up, it progresses any stale WRITING files to WRITTEN or WRITE_ERROR status
        WritingWorkerManager writingWorkerManager = getWritingWorkerManager();
        //Get prep and upload worker managers to execute the flushing
        ManagedPrepWorkerManager managedPrepWorkerManager = getPrepWorkerManager();
        ManagedUploadWorkerManager managedUploadWorkerManager = getUploadWorkerManager();

        try {

            env.start();
            //immediately kick off an initial retry, then continue with regular configured interval
            managedPrepWorkerManager.retryOneThenRetryAll();
            managedUploadWorkerManager.retryOneThenRetryAll();

            ExecutorService executor = Executors.newFixedThreadPool(1);

            //TODO: Configurable timeout. DropWizard will System.exit(1) if we throw an exception from our run method.
            executor.submit(new StateWaiter(logFileTracker)).get();

            System.out.println("[DONE]");

        } finally {
            env.stop();
        }
    }

    private static class StateWaiter implements Runnable, LogFileStateListener {
        private final static Set<LogFileState> STATES_TO_WAIT_ON = ImmutableSet.<LogFileState>of(
                WRITING,
                WRITTEN,
                //don't wait on WRITE_ERROR; probably no data recover
                PREPARING,
                PREPARED,
                PREP_ERROR,
                UPLOADING,
                UPLOAD_ERROR
        );
        private final LogFileTracker tracker;
        private final SynchronousQueue<LogFile> stateChanges;

        private StateWaiter(LogFileTracker tracker) {
            this.tracker = tracker;
            this.stateChanges = new SynchronousQueue<>();
        }

        @Override
        public void run() {
            while (true) {
                int pendingFiles = tracker.getCount(STATES_TO_WAIT_ON);
                if (pendingFiles == 0) {
                    System.out.println("No pending files. Done.");
                    break;
                } else {
                    System.out.println(pendingFiles + " files pending upload. Waiting...");
                    try {
                        //wait until some file's state has changed or until timeout it reached
                        stateChanges.poll(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        System.out.println("Interrupted!");
                        break;
                    }
                }
            }
        }

        @Override
        public void stateChanged(LogFile logFile) {
            //try for a little while to notify run() method that there's a new file, then give up and drop on floor.
            try {
                stateChanges.offer(logFile, 1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                //ignore
            }
        }
    }
}
