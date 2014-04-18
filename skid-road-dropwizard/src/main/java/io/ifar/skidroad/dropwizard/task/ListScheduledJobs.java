package io.ifar.skidroad.dropwizard.task;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import io.ifar.skidroad.scheduling.SimpleQuartzScheduler;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.ISODateTimeFormat;
import org.quartz.TriggerKey;

import java.io.PrintWriter;

/**
 * List Quartz jobs.
 * <code>curl -X POST http://localhost:8081/tasks/list-triggers</code>
 */
public class ListScheduledJobs extends Task {
    final SimpleQuartzScheduler scheduler;

    public ListScheduledJobs(SimpleQuartzScheduler scheduler) {
        super("list-triggers");
        this.scheduler = scheduler;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output) throws Exception {
        output.print("It is now ");
        ISODateTimeFormat.dateHourMinuteSecond().printTo(output, DateTime.now());
        output.println(".");
        for (TriggerKey triggerKey : scheduler.getTriggerKeys()) {
            output.write(triggerKey.getName());
            output.write(" --- last fired: ");
            DateTime lastFired = scheduler.lastFired(triggerKey);
            if (lastFired == null) {
                output.write("<never>");
            } else {
                ISODateTimeFormat.dateHourMinuteSecond().printTo(output, new DateTime(lastFired));
                output.write(" (");
                printDuration(output, lastFired);
                output.write(" ago)");
            }

            output.write(", next fire: ");
            DateTime nextFire = scheduler.nextFire(triggerKey);
            if (nextFire == null) {
                output.write("<never>");
            } else {
                ISODateTimeFormat.dateHourMinuteSecond().printTo(output, new DateTime(nextFire));
                output.write(" (");
                printDuration(output, nextFire);
                output.write(" hence)");
            }

            output.println();
        }
    }

    public void printDuration(PrintWriter output, DateTime dateTime) {
        DateTime now = DateTime.now();
        Duration duration;
        if (now.isAfter(dateTime)) {
            duration = new Duration(dateTime, now);
        } else {
            duration = new Duration(now, dateTime);
        }
        output.print("~");
        if (duration.getStandardHours() > 0) {
            output.print(duration.getStandardHours());
            output.print(" hours");
        } else if (duration.getStandardMinutes() > 0) {
            output.print(duration.getStandardMinutes());
            output.print(" minutes");
        } else {
            output.print(duration.getStandardSeconds());
            output.print(" seconds");
        }
    }
}
