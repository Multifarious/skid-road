package io.ifar.skidroad.dropwizard;

import com.yammer.dropwizard.lifecycle.Managed;
import io.ifar.skidroad.rolling.FileRollingScheme;
import io.ifar.skidroad.scheduling.SimpleQuartzScheduler;
import io.ifar.skidroad.tracking.LogFileTracker;
import io.ifar.skidroad.writing.WritingWorkerManager;
import io.ifar.skidroad.writing.Serializer;
import io.ifar.skidroad.writing.WritingWorkerFactory;

public class ManagedWritingWorkerManager<T> extends WritingWorkerManager<T> implements Managed {

    public ManagedWritingWorkerManager(FileRollingScheme rollingScheme, Serializer<T> serializer, LogFileTracker tracker,
                                       WritingWorkerFactory<T> factory, SimpleQuartzScheduler scheduler, int pruneIntervalMillis,
                                       int spawnThreshold, int unhealthyThreshold) {
        super(rollingScheme,serializer,tracker,factory,scheduler,pruneIntervalMillis,spawnThreshold,unhealthyThreshold);
    }
}
