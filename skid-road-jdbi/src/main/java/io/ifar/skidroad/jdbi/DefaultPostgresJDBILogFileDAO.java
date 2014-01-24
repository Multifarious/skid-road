package io.ifar.skidroad.jdbi;

import io.ifar.skidroad.LogFile;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.customizers.FetchSize;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;

import java.util.Set;

/**
 *
 */
@RegisterMapper({DefaultJDBILogFileMapper.class, CountByStateMapper.class})
public interface DefaultPostgresJDBILogFileDAO extends DefaultJDBILogFileDAO, JDBILogFileDAOWithArraySupport {
    @Override
    @SqlQuery("select rolling_cohort, serial, start_time, origin_uri, prep_uri, archive_key, archive_uri, archive_group," +
            " state, owner_uri, bytes, created_at, updated_at from log_files where owner_uri = :owner_uri and state = ANY(:states) order by start_time asc")
    @FetchSize(50)
    ResultIterator<LogFile> findByOwnerAndState(@Bind("owner_uri") String ownerUri, @Bind(value = "states", binder = StringCollectionBinder.class) Set<String> states);

    @Override
    @SqlQuery("select rolling_cohort, serial, start_time, origin_uri, prep_uri, archive_key, archive_uri, archive_group," +
            " state, owner_uri, bytes, created_at, updated_at from log_files where state = ANY(:states) and start_time >= :first_ts and start_time <= :last_ts" +
            " order by start_time asc")
    @FetchSize(50)
    ResultIterator<LogFile> listLogFilesByDateAndState(
            @Bind(value="states", binder = StringCollectionBinder.class) Set<String> states,
            @Bind("first_ts") DateTime startDate,
            @Bind("last_ts") DateTime endDate);

    @Override
    @SqlQuery("select sum(bytes) from log_files where state = ANY(:states) and start_time >= :first_ts and start_time <= :last_ts")
    Long totalSize(@Bind(value = "states", binder = StringCollectionBinder.class) Set<String> states, @Bind("first_ts") DateTime startDate,
                   @Bind("last_ts") DateTime endDate);

    @Override
    @SqlQuery("select count(*) from log_files where state = ANY(:states)")
    int count(@Bind(value = "states", binder = StringCollectionBinder.class) Set<String> state);

    @Override
    @SqlQuery("select count(*) from log_files where state = ANY(:states) and start_time >= :first_ts and start_time <= :last_ts")
    int count(@Bind(value = "states", binder = StringCollectionBinder.class) Set<String> state, @Bind("first_ts") DateTime startDate,
               @Bind("last_ts") DateTime endDate);

    @Override
    @SqlQuery("select rolling_cohort, serial, start_time, origin_uri, prep_uri, archive_key, archive_uri, archive_group," +
            " state, owner_uri, bytes, created_at, updated_at from log_files" +
            " where owner_uri = :owner, state = ANY(:states) and start_time >= :first_ts and start_time <= :last_ts" +
            " order by start_time asc")
    @FetchSize(50)
    ResultIterator<LogFile> listLogFilesByOwnerAndDateAndState(
            @Bind(value="states", binder = StringCollectionBinder.class) Set<String> states,
            @Bind("owner") String owner,
            @Bind("first_ts") DateTime startDate,
            @Bind("last_ts") DateTime endDate);
}

