package io.ifar.skidroad.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import io.dropwizard.Application;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jdbi.DBIFactory;
import io.dropwizard.jdbi.bundles.DBIExceptionsBundle;
import io.dropwizard.jdbi.logging.LogbackLog;
import io.dropwizard.migrations.MigrationsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.ifar.goodies.Triple;
import io.ifar.skidroad.dropwizard.*;
import io.ifar.skidroad.dropwizard.cli.GenerateRandomKey;
import io.ifar.skidroad.examples.config.SkidRoadDropwizardExampleConfiguration;
import io.ifar.skidroad.examples.rest.ExampleResource;
import io.ifar.skidroad.jdbi.JDBILogFileDAO;
import io.ifar.skidroad.jdbi.JDBILogFileDAOHelper;
import io.ifar.skidroad.jdbi.JodaArgumentFactory;
import io.ifar.skidroad.jersey.ContainerRequestAndResponse;
import io.ifar.skidroad.jersey.combined.capture.RecorderFilter;
import io.ifar.skidroad.jersey.combined.capture.RequestEntityBytesCaptureFilter;
import io.ifar.skidroad.jersey.combined.serialize.JSONContainerRequestAndResponseSerializer;
import io.ifar.skidroad.jersey.headers.CommonHeaderExtractors;
import io.ifar.skidroad.jersey.headers.RequestHeaderExtractor;
import io.ifar.skidroad.jersey.headers.SimpleHeaderExtractor;
import io.ifar.skidroad.jersey.predicate.response.StatusCodeContainerResponsePredicate;
import io.ifar.skidroad.jersey.single.IDTagTripleTransformFactory;
import io.ifar.skidroad.jersey.single.RecorderFilterFactory;
import io.ifar.skidroad.jersey.single.RequestTimestampFilter;
import io.ifar.skidroad.jersey.single.UUIDGeneratorFilter;
import io.ifar.skidroad.writing.WritingWorkerManager;
import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.sql.SQLException;

/**

 */
public class SkidRoadDropwizardExampleService extends Application<SkidRoadDropwizardExampleConfiguration> {
    public static void main(String... args) throws Exception {
        new SkidRoadDropwizardExampleService().run(args);
    }

    private final static Logger LOG = LoggerFactory.getLogger(SkidRoadDropwizardExampleService.class);
    @Override
    public void initialize(Bootstrap<SkidRoadDropwizardExampleConfiguration> bootstrap) {

        ObjectMapper om = bootstrap.getObjectMapper();
        om.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        om.registerModule(new JodaModule());
        om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

        bootstrap.addBundle(new DBIExceptionsBundle());
        bootstrap.addBundle(new MigrationsBundle<SkidRoadDropwizardExampleConfiguration>() {
            @Override
            public DataSourceFactory getDataSourceFactory(SkidRoadDropwizardExampleConfiguration configuration) {
                return configuration.getSkidRoad().getDatabaseConfiguration();
            }
        });
        bootstrap.addCommand(new GenerateRandomKey());
    }

    @Override
    public void run(final SkidRoadDropwizardExampleConfiguration configuration, Environment environment) throws Exception {

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
        JDBILogFileDAO dao = jdbi.onDemand(JDBILogFileDAOHelper.bestDefaultDAOForDriver(configuration.getSkidRoad().getDatabaseConfiguration().getDriverClass()));

        ManagedJDBILogFileTracker tracker = new ManagedJDBILogFileTracker(new URI("http://" + configuration.getSkidRoad().getNodeId()), dao);
        environment.lifecycle().manage(tracker);

        ManagedUploadWorkerManager.build(
                configuration.getSkidRoad(),
                environment,
                tracker,
                ManagedAwsS3ClientStorage.buildWorkerFactory(configuration.getSkidRoad(), environment));

        ManagedPrepWorkerManager.buildWithEncryptAndCompress(configuration.getSkidRoad(), environment, tracker);

        WritingWorkerManager<ContainerRequestAndResponse> writerManager = ManagedWritingWorkerManager.build(
                tracker,
                new JSONContainerRequestAndResponseSerializer(Jackson.newObjectMapper())
                        .with((RequestHeaderExtractor) SimpleHeaderExtractor.only("User-Agent")) //only include the User-Agent request header
                        .with(CommonHeaderExtractors.NO_RESPONSE_HEADERS) //don't headers response headers
                ,
                configuration.getSkidRoad(),
                environment);

        JerseyFilterHelper.addFilter(environment, new RequestEntityBytesCaptureFilter());
        //Only request when result is successful.
        JerseyFilterHelper.addFilter(environment, new RecorderFilter(StatusCodeContainerResponsePredicate.SUCCESS_PREDICATE, writerManager));



        //Slightly unconventional: add a second WriterWorkerManager to demonstrate alternate serialization
        WritingWorkerManager<Triple<String,String,String>> csvWriterManager = ManagedWritingWorkerManager.buildCSV(
                tracker,
                "",
                configuration.getSkidRoad().getRequestLogWriterConfiguration().copy().setNameSuffix(".csv"),
                environment);

        //generate a UUID for each request to use in all CSV rows
        JerseyFilterHelper.addFilter(environment, new UUIDGeneratorFilter());
        //generate a timestamp to determine which output file will contain recordings for this request
        JerseyFilterHelper.addFilter(environment, new RequestTimestampFilter());

        //write the timestamp to the CSV file
        JerseyFilterHelper.addFilter(environment, RecorderFilterFactory.build(
                RecorderFilterFactory.EXTRACT_REQUEST_TIMESTAMP,
                IDTagTripleTransformFactory.<String>isoDateTime("TIMESTAMP"),
                csvWriterManager
        ));
        //write the request body to the CSV file
        JerseyFilterHelper.addFilter(environment, RecorderFilterFactory.build(
                RecorderFilterFactory.EXTRACT_REQUEST_BODY,
                IDTagTripleTransformFactory.<String>passThrough("REQUEST_BODY"),
                csvWriterManager
        ));

        environment.jersey().register(new ExampleResource(csvWriterManager));
    }
}
