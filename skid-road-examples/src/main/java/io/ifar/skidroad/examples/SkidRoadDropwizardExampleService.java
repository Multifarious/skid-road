package io.ifar.skidroad.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import io.dropwizard.Application;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.jdbi.DBIFactory;
import io.dropwizard.jdbi.bundles.DBIExceptionsBundle;
import io.dropwizard.jdbi.logging.LogbackLog;
import io.dropwizard.migrations.MigrationsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.ifar.goodies.Pair;
import io.ifar.skidroad.dropwizard.*;
import io.ifar.skidroad.dropwizard.cli.GenerateRandomKey;
import io.ifar.skidroad.examples.config.SkidRoadDropwizardExampleConfiguration;
import io.ifar.skidroad.examples.rest.ExampleResource;
import io.ifar.skidroad.examples.rest.RainbowRequest;
import io.ifar.skidroad.examples.rest.RainbowRequestResponse;
import io.ifar.skidroad.jdbi.JDBILogFileDAO;
import io.ifar.skidroad.jdbi.JDBILogFileDAOHelper;
import io.ifar.skidroad.jdbi.JodaArgumentFactory;
import io.ifar.skidroad.writing.WritingWorkerManager;
import io.ifar.skidroad.writing.file.Serializer;
import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    public void run(final SkidRoadDropwizardExampleConfiguration configuration, final Environment environment) throws Exception {

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


        WritingWorkerManager<Pair<RainbowRequest,RainbowRequestResponse>> writerManager = ManagedWritingWorkerManager.build(
                tracker,
                new Serializer<Pair<RainbowRequest, RainbowRequestResponse>>() {
                    @Override
                    public String serialize(Pair<RainbowRequest, RainbowRequestResponse> item) throws IOException {
                        return environment.getObjectMapper().writeValueAsString(item);
                    }
                },
                configuration.getSkidRoad(),
                environment);

        environment.jersey().register(new ExampleResource(writerManager));

    }
}
