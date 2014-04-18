package io.ifar.skidroad.dropwizard;

import io.dropwizard.lifecycle.Managed;
import io.ifar.skidroad.jdbi.JDBILogFileDAO;
import io.ifar.skidroad.jdbi.JDBILogFileTracker;

import java.net.URI;

public class ManagedJDBILogFileTracker extends JDBILogFileTracker implements Managed {

    public ManagedJDBILogFileTracker(URI localURI, JDBILogFileDAO dao) {
        super(localURI, dao);
    }
}
