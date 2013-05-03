package io.ifar.skidroad.scheduling;

import org.quartz.*;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spi.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight lifecycle wrapper around Quartz scheduler.
 */
public class SimpleQuartzScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleQuartzScheduler.class);
    private final Scheduler scheduler;
    private final String schedulerName;
    private final static AtomicLong NEXT_SCHEDULER_ID = new AtomicLong(1L);

    /**
     * Creates a new SimpleQuartzScheduler
     * @param schedulerName must be unique for this JVM
     * @param maxThreads
     * @throws SchedulerException
     */
    public SimpleQuartzScheduler(String schedulerName, int maxThreads) throws SchedulerException {
        SimpleThreadPool threadPool = new SimpleThreadPool(maxThreads, Thread.NORM_PRIORITY);
        threadPool.initialize();
        JobStore jobStore = new RAMJobStore();
        String schedulerInstanceID = Long.toString(NEXT_SCHEDULER_ID.getAndIncrement());
        DirectSchedulerFactory.getInstance().createScheduler(
                schedulerName,
                schedulerInstanceID, //not sure what this accomplishes
                threadPool,
                jobStore);
        this.scheduler = DirectSchedulerFactory.getInstance().getScheduler(schedulerName);
        this.schedulerName = schedulerName;
    }

    public void start() throws Exception {
        scheduler.start();
    }

    public void stop() throws Exception {
        scheduler.shutdown();
    }

    /**
     * Schedules execution of the provided job. Rather than calling this directly,
     * see the other schedule methods that create a ScheduleBuilder object based on
     * simpler parameters.
     *
     * @param jobName arbitrary unique name
     * @param jobClass Job to execute
     * @param schedule Schedule to execute on
     * @param jobConfig Quartz jobConfig made available to jobClass instance when it executes.
     * @see org.quartz.Job
     */
    public <T extends Job, SBT extends Trigger, K, V> Trigger schedule(String jobName, Class<T> jobClass, ScheduleBuilder<SBT> schedule, Map<K,V> jobConfig) {
        JobDataMap jobDataMap = new JobDataMap(jobConfig);

        JobDetail jobDetail = JobBuilder.
                newJob().
                withIdentity(jobName, schedulerName).
                ofType(jobClass).
                usingJobData(jobDataMap).
                build();

        Trigger trigger = TriggerBuilder.
                newTrigger().
                withIdentity(jobName, schedulerName).
                withSchedule(schedule).
                build();

        try {
            scheduler.scheduleJob(jobDetail, trigger);
            return trigger;
        } catch (SchedulerException e) {
            LOG.error("{} was NOT scheduled", trigger.getKey(), e);
            return null;
        }
    }

    /**
     * Schedules execution of the provided job.
     *
     * @param cron E.g. "0 0/5 14,18 * * ?" for every five minutes during the 14:00 and 18:00 hours.
     * @throws ParseException
     * @see org.quartz.CronTrigger
     */
    public <T extends Job, K,V> Trigger schedule(String name, Class<T> jobClass, String cron, Map<K,V> jobConfig) throws ParseException {
        return schedule(name, jobClass, CronScheduleBuilder.cronSchedule(cron), jobConfig);
    }

    /**
     * Provide access to underlying Quartz Scheduler
     */
    public Scheduler getScheduler() {
        return scheduler;
    }

    /**
     * Schedules execution of the provided job.
     * @param intervalMillis Interval in milliseconds between job executions.
     */
    public <T extends Job, K,V> Trigger schedule(String name, Class<T> jobClass, int intervalMillis, Map<K,V> jobConfig) {
        return schedule(name, jobClass, SimpleScheduleBuilder.simpleSchedule().withIntervalInMilliseconds(intervalMillis).repeatForever(), jobConfig);
    }

    public boolean unschedule(Trigger trigger)  {
        try {
            return scheduler.unscheduleJob(trigger.getKey());
        } catch (SchedulerException e) {
            LOG.error("{} was NOT unscheduled", trigger.getKey(), e);
            return false;
        }
    }


    public void clear() {
        try {
            scheduler.clear();
        } catch (SchedulerException e) {
            LOG.error("Error unscheduling all jobs", e);
        }
    }

    public boolean fire(Trigger trigger) {
        try {
            scheduler.triggerJob(trigger.getJobKey());
            return true;
        } catch (SchedulerException e) {
            LOG.error("{} was NOT triggered", trigger.getKey(), e);
            return false;
        }
    }
}
