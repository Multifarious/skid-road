package io.ifar.skidroad.jdbi;

import io.ifar.skidroad.LogFile;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.ResultIterator;

import java.sql.Timestamp;
import java.util.List;

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

    ResultIterator<LogFile> findByOwnerAndState(String ownerUri, String state);

    ResultIterator<String> listOwnerUris();

    List<CountByState> countLogFilesByState();

    int count(String state);

    Long totalSize(String state, DateTime startDate, DateTime endDate);

    int count(String state, DateTime startDate, DateTime endDate);

    int count(DateTime startDate, DateTime endDate);

    ResultIterator<LogFile> listLogFilesByDateAndState(String state, DateTime startDate, DateTime endDate);

    ResultIterator<LogFile> listLogFilesByDate(DateTime startDate, DateTime endDate);

    LogFile findByRollingCohortAndSerial(String rollingCohort, int serial);

    void close();
}
