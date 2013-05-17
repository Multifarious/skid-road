package io.ifar.skidroad.jdbi;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileState;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 *
 */
public class DefaultJDBILogFileMapper implements ResultSetMapper<LogFile> {
    @Override
    public LogFile map(int index, ResultSet r, StatementContext ctx) throws SQLException {
        int col = 1;
        return new LogFile(
                r.getString(col++),
                r.getInt(col++),
                parseDateTime(r.getTimestamp(col++)),
                parsePathFromUri(r.getString(col++)),
                parsePathFromUri(r.getString(col++)),
                r.getString(col++),
                parseUri(r.getString(col++)),
                r.getString(col++),
                LogFileState.valueOf(r.getString(col++)),
                parseUri(r.getString(col++)),
                r.getLong(col++),
                parseDateTime(r.getTimestamp(col++)),
                parseDateTime(r.getTimestamp(col))
        );
    }

    private Path parsePathFromUri(String uri) throws SQLException {
        URI parsedURI = parseUri(uri);
        return parsedURI == null ? null : Paths.get(parsedURI);
    }
    private DateTime parseDateTime(Timestamp sqlTimestamp) {
        return sqlTimestamp == null ? null : new DateTime(sqlTimestamp.getTime());
    }

    private URI parseUri(String uri) throws SQLException {
        try {
            return uri == null ? null : new URI(uri);
        } catch (URISyntaxException e) {
            throw new SQLException("Invalid URI " + uri,  e);
        }
    }
}
