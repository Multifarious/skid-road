package io.ifar.skidroad.dropwizard.cli;

import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.jdbi.DBIFactory;
import io.ifar.skidroad.dropwizard.config.SkidRoadReadOnlyConfigurationStrategy;
import io.ifar.goodies.CliConveniences;
import io.ifar.skidroad.jdbi.CountByState;
import io.ifar.skidroad.jdbi.DefaultJDBILogFileDAO;
import io.ifar.skidroad.jdbi.JDBILogFileDAO;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.commons.lang.StringUtils;
import org.skife.jdbi.v2.DBI;

import java.util.List;

/**
 *
 */
public abstract class ListStatesCommand<T extends Configuration> extends ConfiguredCommand<T>
        implements SkidRoadReadOnlyConfigurationStrategy<T> {

    public ListStatesCommand() {
        super("list-states","List the file states currently in the database with counts.");
    }

    @Override
    protected void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {
        CliConveniences.quietLogging("ifar", "hsqldb.db");
        Environment env = CliConveniences.fabricateEnvironment(getName(), configuration);
        env.start();
        try {
            final DBIFactory factory = new DBIFactory();
            final DBI jdbi = factory.build(env, getSkidRoadReadOnlyConfiguration(configuration).getDatabaseConfiguration(), "logfile");
            JDBILogFileDAO dao = jdbi.onDemand(DefaultJDBILogFileDAO.class);
            List<CountByState> counts = dao.countLogFilesByState();
            if (!counts.isEmpty()) {
                System.out.println(String.format("%-20s | %-10s","STATE","COUNT"));
                System.out.println(String.format("%s_|_%s", StringUtils.repeat("_",20),StringUtils.repeat("_",10)));
            }
            for (CountByState cbs : counts) {
                System.out.println(String.format("%-20s | %-10d",cbs.getState(),cbs.getCount()));
            }
        } finally {
            env.stop();
        }
    }
}
