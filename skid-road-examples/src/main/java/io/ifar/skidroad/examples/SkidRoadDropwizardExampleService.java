package io.ifar.skidroad.examples;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.db.DatabaseConfiguration;
import com.yammer.dropwizard.jdbi.DBIFactory;
import com.yammer.dropwizard.jdbi.bundles.DBIExceptionsBundle;
import com.yammer.dropwizard.jdbi.logging.LogbackLog;
import com.yammer.dropwizard.json.ObjectMapperFactory;
import com.yammer.dropwizard.migrations.MigrationsBundle;
import io.ifar.skidroad.dropwizard.*;
import io.ifar.skidroad.examples.config.SkidRoadDropwizardExampleConfiguration;
import io.ifar.skidroad.examples.rest.ExampleResource;
import io.ifar.skidroad.jdbi.DefaultJDBILogFileDAO;
import io.ifar.skidroad.jdbi.JDBILogFileDAO;
import io.ifar.skidroad.jdbi.JodaArgumentFactory;
import io.ifar.skidroad.jersey.*;
import io.ifar.skidroad.jersey.capture.RequestEntityBytesCaptureFilter;
import io.ifar.skidroad.jersey.capture.SkidRoadFilter;
import io.ifar.skidroad.jersey.serialize.DefaultContainerRequestAndResponseSerializer;
import io.ifar.skidroad.scheduling.SimpleQuartzScheduler;
import io.ifar.skidroad.writing.WritingWorkerManager;
import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.sql.SQLException;

/**

 */
public class SkidRoadDropwizardExampleService extends Service<SkidRoadDropwizardExampleConfiguration> {
    public static void main(String... args) throws Exception {
        new SkidRoadDropwizardExampleService().run(args);
    }

    private final static Logger LOG = LoggerFactory.getLogger(SkidRoadDropwizardExampleService.class);
    @Override
    public void initialize(Bootstrap<SkidRoadDropwizardExampleConfiguration> bootstrap) {

        ObjectMapperFactory omf = bootstrap.getObjectMapperFactory();
        omf.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        omf.registerModule(new JodaModule());
        omf.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

        bootstrap.addBundle(new DBIExceptionsBundle());
        bootstrap.addBundle(new MigrationsBundle<SkidRoadDropwizardExampleConfiguration>() {
            @Override
            public DatabaseConfiguration getDatabaseConfiguration(SkidRoadDropwizardExampleConfiguration configuration) {
                return configuration.getSkidRoad().getDatabaseConfiguration();
            }
        });
    }

    @Override
    public void run(SkidRoadDropwizardExampleConfiguration configuration, Environment environment) throws Exception {
        SimpleQuartzScheduler scheduler = ManagedSimpleQuartzScheduler.build(configuration.getSkidRoad(), environment);

        final DBIFactory factory = new DBIFactory();
        final DBI jdbi = factory.build(environment, configuration.getSkidRoad().getDatabaseConfiguration(), "skid-road-example");
        jdbi.registerArgumentFactory(new JodaArgumentFactory());
        jdbi.setSQLLog(new LogbackLog());
        // Fail fast on database connectivity.
        try {
            jdbi.open().getConnection().close();
        } catch (SQLException se) {
            LOG.error("Database connectivity error; unable to get a connection from a freshly initialized pool.",se);
            throw new RuntimeException(se);
        }
        JDBILogFileDAO dao = jdbi.onDemand(DefaultJDBILogFileDAO.class);

        ManagedJDBILogFileTracker tracker = new ManagedJDBILogFileTracker(new URI("http://" + configuration.getSkidRoad().getNodeId()), dao);
        environment.manage(tracker);

        ManagedUploadWorkerManager.build(
                configuration.getSkidRoad(),
                environment,
                tracker,
                ManagedS3JetS3tStorage.buildWorkerFactory(configuration.getSkidRoad(), environment),
                scheduler);

        ManagedPrepWorkerManager.buildWithEncryptAndCompress(configuration.getSkidRoad(), environment, tracker, scheduler);

        WritingWorkerManager<ContainerRequestAndResponse> writerManager = ManagedWritingWorkerManager.build(
                tracker, new DefaultContainerRequestAndResponseSerializer(environment.getObjectMapperFactory().build()), scheduler, configuration.getSkidRoad(), environment);

        JerseyFilterHelper.addFilter(environment, new RequestEntityBytesCaptureFilter());
        JerseyFilterHelper.addFilter(environment, new SkidRoadFilter(writerManager));

        environment.addResource(new ExampleResource());
    }
}
