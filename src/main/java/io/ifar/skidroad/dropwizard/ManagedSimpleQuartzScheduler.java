package io.ifar.skidroad.dropwizard;

import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.lifecycle.Managed;
import com.yammer.metrics.core.HealthCheck;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfiguration;
import io.ifar.skidroad.scheduling.SimpleQuartzScheduler;
import org.joda.time.format.ISODateTimeFormat;
import org.quartz.Job;
import org.quartz.ScheduleBuilder;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

/**
 * Lightweight lifecycle wrapper around Quartz scheduler.
 */
public class ManagedSimpleQuartzScheduler extends SimpleQuartzScheduler implements Managed {
    private static final Logger LOG = LoggerFactory.getLogger(ManagedSimpleQuartzScheduler.class);
    private final Environment environment;

    public ManagedSimpleQuartzScheduler(String group, int maxThreads, Environment environment) throws SchedulerException {
        super(group,maxThreads);
        this.environment = environment;
    }
    public ManagedSimpleQuartzScheduler(int maxThreads, Environment environment) throws SchedulerException {
        this(ManagedSimpleQuartzScheduler.class.getSimpleName(), maxThreads, environment);
    }

    public static ManagedSimpleQuartzScheduler build(SkidRoadConfiguration skidRoadConfiguration, Environment environment) throws SchedulerException {
        ManagedSimpleQuartzScheduler scheduler = new ManagedSimpleQuartzScheduler(skidRoadConfiguration.getMaxQuartzThreads(),environment);
        environment.manage(scheduler);
        return scheduler;
    }

    @Override
    public <T extends Job, SBT extends Trigger, K, V> Trigger schedule(String jobName, Class<T> jobClass, ScheduleBuilder<SBT> schedule, Map<K,V> jobConfig) {
        Trigger trigger = super.schedule(jobName, jobClass, schedule, jobConfig);
        if (trigger != null)
            registerHealthCheck(environment, trigger);
        return trigger;
    }

    private static void registerHealthCheck(Environment environment, final Trigger trigger) {
        environment.addHealthCheck(new HealthCheck(trigger.getJobKey() + "_next_run") {
            protected Result check() throws Exception {
                Date nextFire = trigger.getNextFireTime();
                return Result.healthy(
                        nextFire == null ? "<never>" :
                                ISODateTimeFormat.basicDateTimeNoMillis().print(nextFire.getTime()));
            }
        });
        environment.addHealthCheck(new HealthCheck(trigger.getJobKey() + "_last_ran") {
            protected Result check() throws Exception {
                Date nextFire = trigger.getPreviousFireTime();
                return Result.healthy(
                        nextFire == null ? "<never>" :
                                ISODateTimeFormat.basicDateTimeNoMillis().print(nextFire.getTime()));
            }
        });
    }
}
