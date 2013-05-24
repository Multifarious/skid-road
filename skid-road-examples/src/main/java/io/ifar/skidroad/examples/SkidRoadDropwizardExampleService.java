package io.ifar.skidroad.examples;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Function;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.db.DatabaseConfiguration;
import com.yammer.dropwizard.jdbi.DBIFactory;
import com.yammer.dropwizard.jdbi.bundles.DBIExceptionsBundle;
import com.yammer.dropwizard.jdbi.logging.LogbackLog;
import com.yammer.dropwizard.json.ObjectMapperFactory;
import com.yammer.dropwizard.migrations.MigrationsBundle;
import io.ifar.goodies.Triple;
import io.ifar.skidroad.dropwizard.*;
import io.ifar.skidroad.examples.config.SkidRoadDropwizardExampleConfiguration;
import io.ifar.skidroad.examples.rest.ExampleResource;
import io.ifar.skidroad.jdbi.DefaultJDBILogFileDAO;
import io.ifar.skidroad.jdbi.JDBILogFileDAO;
import io.ifar.skidroad.jdbi.JodaArgumentFactory;
import io.ifar.skidroad.jersey.combined.ContainerRequestAndResponse;
import io.ifar.skidroad.jersey.combined.capture.RecorderFilter;
import io.ifar.skidroad.jersey.combined.capture.RequestEntityBytesCaptureFilter;
import io.ifar.skidroad.jersey.combined.serialize.JSONContainerRequestAndResponseSerializer;
import io.ifar.skidroad.jersey.headers.CommonHeaderExtractors;
import io.ifar.skidroad.jersey.headers.RequestHeaderExtractor;
import io.ifar.skidroad.jersey.headers.SimpleHeaderExtractor;
import io.ifar.skidroad.jersey.predicate.response.StatusCodeContainerResponsePredicate;
import io.ifar.skidroad.jersey.single.*;
import io.ifar.skidroad.scheduling.SimpleQuartzScheduler;
import io.ifar.skidroad.writing.WritingWorkerManager;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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
    public void run(final SkidRoadDropwizardExampleConfiguration configuration, Environment environment) throws Exception {
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
                tracker,
                new JSONContainerRequestAndResponseSerializer(environment.getObjectMapperFactory().build())
                        .with((RequestHeaderExtractor) SimpleHeaderExtractor.only("User-Agent")) //only include the User-Agent request header
                        .with(CommonHeaderExtractors.NO_RESPONSE_HEADERS) //don't headers response headers
                ,
                scheduler,
                configuration.getSkidRoad(),
                environment);

        JerseyFilterHelper.addFilter(environment, new RequestEntityBytesCaptureFilter());
        //Only request when result is successful.
        JerseyFilterHelper.addFilter(environment, new RecorderFilter(StatusCodeContainerResponsePredicate.SUCCESS_PREDICATE, writerManager));



        //Slightly unconventional: add a second WriterWorkerManager to demonstrate alternate serialization
        WritingWorkerManager<Triple<String,String,String>> csvWriterManager = ManagedWritingWorkerManager.buildCSV(
                tracker,
                "",
                scheduler,
                configuration.getSkidRoad().getRequestLogWriterConfiguration().copy().setNameSuffix(".csv"),
                environment);

        //generate a UUID for each request
        JerseyFilterHelper.addFilter(environment, new UUIDGeneratorFilter());
        //generate a timestamp to use for all recordings of this request
        JerseyFilterHelper.addFilter(environment, new RequestTimestampFilter());
        //write the timestamp to the CSV file
        JerseyFilterHelper.addFilter(environment, new RequestTimestampRecorder<>(
                IDTypeTripleTransformFactory.<DateTime,String>buildTransform(
                        RequestTimestampRecorder.TIMESTAMP_TYPE_KEY,
                        new Function<DateTime,String>() {
                            @Nullable
                            @Override
                            public String apply(@Nullable DateTime input) {
                                return ISODateTimeFormat.basicDateTime().print(input);
                            }
                        }
                        ),
                csvWriterManager
        ));
        //write the request body to the CSV file
        JerseyFilterHelper.addFilter(environment, new RequestEntityRecorder<>(
                IDTypeTripleTransformFactory.<String>buildTransform(RequestEntityRecorder.REQUEST_TYPE_KEY),
                csvWriterManager
        ));

        environment.addResource(new ExampleResource(csvWriterManager));
    }
}
