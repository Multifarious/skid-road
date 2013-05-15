package io.ifar.skidroad.jdbi;

import io.ifar.skidroad.LogFile;
import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Set;

/**
 * DAO used by JDBILogFileTracker. Either use {@link DefaultJDBILogFileDAO} with the standard table layout or implement your
 * own.  Don't forget to add the {@link JodaArgumentFactory} to your {@link org.skife.jdbi.v2.DBI} instance.
 */
public interface JDBILogFileDAO {

    int updateState(String rollingCohort, int serial, String state, String expectedOwner, Timestamp now);

    int updatePrepPath(String rollingCohort, int serial, String prepUri, String expectedOwner, Timestamp now);

    int updateArchiveKey(String rollingCohort, int serial, String archiveKey, String expectedOwner, Timestamp now);

    int updateArchiveLocation(String rollingCohort, int serial, String archiveGroup, String archiveURI, String expectedOwner, Timestamp now);

    int updateSize(String rollingCohort, int serial, Long byteSize, String expectedOwner, Timestamp now);

    int determineNextSerial(String rollingCohort);

    int claimIndex(String rollingCohort, int serial, Timestamp startTime, String originUri, String ownerUri, Timestamp now);

    Iterator<LogFile> findByOwnerAndState(String ownerUri, String state);

    Iterator<String> listOwnerUris();

    Iterator<CountByState> countLogFilesByState();

    int countLogFilesByState(String state);

    Long totalSize(Set<String> states, DateTime startDate, DateTime endDate);

    Long count(Set<String> states, DateTime startDate, DateTime endDate);

    Iterator<LogFile> listLogFilesByDateAndState(Set<String> state, DateTime startDate, DateTime endDate);

    LogFile findByRollingCohortAndSerial(String rollingCohort, int serial);

    void close();
}
