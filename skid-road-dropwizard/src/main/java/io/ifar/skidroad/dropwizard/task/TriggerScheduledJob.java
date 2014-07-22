package io.ifar.skidroad.dropwizard.task;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import io.ifar.skidroad.scheduling.SimpleQuartzScheduler;

import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

/**
 * Trigger a Quartz job, e.g.:
 * <code>curl -X POST http://localhost:8081/tasks/fire-trigger\?job\=my_job\&amp;job\=my_other_job</code>
 */
public class TriggerScheduledJob extends Task {
    SimpleQuartzScheduler scheduler;

    public TriggerScheduledJob(SimpleQuartzScheduler scheduler) {
        super("fire-trigger");
        this.scheduler = scheduler;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output) throws Exception {
        ImmutableCollection<String> requestedTriggerNames = parameters.get("job");
        if (requestedTriggerNames == null || requestedTriggerNames.isEmpty()) {
            output.println("1 or more jobs must be specified via 'job' request parameter. Nothing fired.");
            return;
        }

        Set<String> knownTriggerNames = scheduler.getTriggerNames();
        Set<String> toFire = new HashSet<>(requestedTriggerNames.size());
        for (String triggerName : requestedTriggerNames) {
            if (knownTriggerNames.contains(triggerName)) {
                toFire.add(triggerName);
            } else {
                output.println("Unknown job " + triggerName);
            }
        }
        if (toFire.size() == requestedTriggerNames.size()) {
            for (String triggerName : toFire) {
                output.println("Firing " + triggerName);
                scheduler.fire(triggerName);
            }
            output.println("Done.");
        } else {
            output.println("Nothing fired.");
        }
    }


}
