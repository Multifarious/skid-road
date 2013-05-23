package io.ifar.skidroad.jdbi;

import com.google.common.collect.Iterators;
import io.ifar.goodies.AutoCloseableIterator;
import io.ifar.goodies.JdbiAutoCloseableIterator;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileState;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.*;

/**
 * Provides abstraction to gloss over JDBILogFileDAO vs. JDBILogFileDAOWithArraySupport.
 *
 * Note that methods which return iterators will fetch all results into memory when used
 * without JDBILogFileDAOWithArraySupport. Not advised for production use.
 *
 * TODO: consolidate duplicate code
 */
public class JDBILogFileDAOHelper {

    public static Class<? extends DefaultJDBILogFileDAO> bestDefaultDAOForDriver(String jdbcDriverClass) {
        if ("org.postgresql.Driver".equals(jdbcDriverClass)) {
            //supports arrays
            return DefaultPostgresJDBILogFileDAO.class;
        } else {
            return DefaultJDBILogFileDAO.class;
        }
    }

    public static AutoCloseableIterator<LogFile> findByOwnerAndState(JDBILogFileDAO dao, URI owner, Set<LogFileState> states) {
        if (dao instanceof JDBILogFileDAOWithArraySupport) {
            Set<String> stateStrings = new HashSet<>(states.size());
            for (LogFileState state : states) {
                stateStrings.add(state.toString());
            }
            return JdbiAutoCloseableIterator.wrap(
                    ((JDBILogFileDAOWithArraySupport)dao).findByOwnerAndState(
                            owner.toString(),
                            stateStrings));
        } else if (states.size() == 1) {
            return JdbiAutoCloseableIterator.wrap( dao.findByOwnerAndState(owner.toString(), states.iterator().next().toString()) );
        } else {
            Iterator<LogFile> result = null;
            for (LogFileState state : states) {
                ArrayList<LogFile> fetched = new ArrayList<>();
                Iterators.addAll(fetched, dao.findByOwnerAndState(owner.toString(), state.toString()));
                result = (result == null) ? fetched.iterator() : Iterators.concat(result, fetched.iterator());
            }
            return new AutoCloseableIterator<LogFile>(result);
        }
    }

    public static AutoCloseableIterator<LogFile> listLogFilesByDateAndState(JDBILogFileDAO dao, Set <String> states, DateTime startDate, DateTime endDate) {
        if (dao instanceof JDBILogFileDAOWithArraySupport) {
            return JdbiAutoCloseableIterator.wrap(((JDBILogFileDAOWithArraySupport) dao).listLogFilesByDateAndState(states, startDate, endDate));
        } else if (states.size() == 1) {
            return JdbiAutoCloseableIterator.wrap(
                    dao.listLogFilesByDateAndState(states.iterator().next(), startDate, endDate)
            );
        } else {
            //May be the case that it isn't possible to hold multiple database cursors open at once:
            //java.sql.SQLException: invalid cursor state: identified cursor is not open
            //Therefore drain each iterator fully. (Alternatively could implement smart iterator that doesn't issue
            //next query until prior query exhausted...)
            Iterator<LogFile> result = null;
            for (String state : states) {
                ArrayList <LogFile> fetched = new ArrayList<>();
                Iterators.addAll(fetched, dao.listLogFilesByDateAndState(state, startDate, endDate));
                result = (result == null) ? fetched.iterator() : Iterators.concat(result, fetched.iterator());
            }
            return new AutoCloseableIterator<LogFile>(result);
        }
    }

    public static long count(JDBILogFileDAO dao, Set<String> states, DateTime startDate, DateTime endDate) {
        if (dao instanceof JDBILogFileDAOWithArraySupport) {
            return ((JDBILogFileDAOWithArraySupport)dao).count(states, startDate, endDate);
        } else if (states.size() == 1) {
            return dao.count(states.iterator().next(), startDate, endDate);
        } else {
            long result = 0;
            for (String state : states) {
                result += dao.count(state, startDate, endDate);
            }
            return result;
        }
    }

    public static long totalSize(JDBILogFileDAO dao, Set<String> states, DateTime startDate, DateTime endDate) {
        if (dao instanceof JDBILogFileDAOWithArraySupport) {
            return ((JDBILogFileDAOWithArraySupport)dao).totalSize(states, startDate, endDate);
        } else if (states.size() == 1) {
            return dao.totalSize(states.iterator().next(), startDate, endDate);
        } else {
            long result = 0;
            for (String state : states) {
                result += dao.totalSize(state, startDate, endDate);
            }
            return result;
        }
    }
}
