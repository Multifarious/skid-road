package io.ifar.skidroad.jdbi;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 */
public class CountByStateMapper implements ResultSetMapper<CountByState> {

    @Override
    public CountByState map(int index, ResultSet r, StatementContext ctx) throws SQLException {
        int col = 1;
        return new CountByState(r.getString(col++),r.getInt(col));
    }
}
