package io.ifar.skidroad.jdbi;

import io.ifar.skidroad.LogFile;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.ResultIterator;

import java.util.Set;

/**
 * Extend JDBILogFileDAO for more efficient queries against Databases such as Postgres that have array support.
 */
public interface JDBILogFileDAOWithArraySupport extends JDBILogFileDAO {

    ResultIterator<LogFile> findByOwnerAndState(String ownerUri, Set<String> states);

    Long totalSize(Set<String> states, DateTime startDate, DateTime endDate);

    Long count(Set<String> states, DateTime startDate, DateTime endDate);

    ResultIterator<LogFile> listLogFilesByDateAndState(Set<String> state, DateTime startDate, DateTime endDate);

    void close();
}
