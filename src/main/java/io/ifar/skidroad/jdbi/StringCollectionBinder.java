package io.ifar.skidroad.jdbi;

import com.google.common.base.Joiner;
import org.skife.jdbi.v2.SQLStatement;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.Binder;

import java.sql.Array;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;

/**
 *
 */
public class StringCollectionBinder implements Binder<Bind,Collection<String>> {
    @Override
    public void bind(SQLStatement<?> q, Bind bind, Collection<String> arg) {
        Array a = null;
        if (arg != null) {
            try {
                a = q.getContext().getConnection().createArrayOf("varchar",arg.toArray());
            } catch (SQLException se) {
                throw new RuntimeException(String.format("Unable to bind collection {%s} to a SQL array: (%s) %s",
                        Joiner.on(" ,").join(arg), se.getClass(), se.getMessage()),se);
            }
        }
        q.bindBySqlType(bind.value(),a, Types.ARRAY);
    }
}
