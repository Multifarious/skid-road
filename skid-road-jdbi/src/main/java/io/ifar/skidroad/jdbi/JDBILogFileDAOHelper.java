package io.ifar.skidroad.jdbi;

import com.google.common.collect.Iterators;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileState;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.ResultIterator;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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

    public static ResultIterator<LogFile> findByOwnerAndState(JDBILogFileDAO dao, URI owner, Set<LogFileState> states) {
        if (dao instanceof JDBILogFileDAOWithArraySupport) {
            Set<String> stateStrings = new HashSet<>(states.size());
            for (LogFileState state : states) {
                stateStrings.add(state.toString());
            }
            return ((JDBILogFileDAOWithArraySupport)dao).findByOwnerAndState(
                    owner.toString(),
                    stateStrings);
        } else if (states.size() == 1) {
            return dao.findByOwnerAndState(owner.toString(), states.iterator().next().toString());
        } else {
            Iterator<LogFile> accumulator = null;
            for (LogFileState state : states) {
                ArrayList<LogFile> fetched = new ArrayList<>();
                Iterators.addAll(fetched, dao.findByOwnerAndState(owner.toString(), state.toString()));
                accumulator = (accumulator == null) ? fetched.iterator() : Iterators.concat(accumulator, fetched.iterator());
            }
            final Iterator<LogFile> result = accumulator == null ? Iterators.<LogFile>emptyIterator() : accumulator;
            return new ResultIterator<LogFile>() {
                @Override
                public void close() {
                    // no op for this one.
                }

                @Override
                public boolean hasNext() {
                    return result.hasNext();
                }

                @Override
                public LogFile next() {
                    return result.next();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    public static ResultIterator<LogFile> listLogFilesByOwnerAndDateAndState(JDBILogFileDAO dao, URI owner,
                                                                             Set <LogFileState> states, DateTime startDate, DateTime endDate) {
        if (dao instanceof JDBILogFileDAOWithArraySupport) {
            Set<String> stateNames = new HashSet<>();
            for (LogFileState state : states) {
                stateNames.add(state.name());
            }
            return ((JDBILogFileDAOWithArraySupport) dao).listLogFilesByOwnerAndDateAndState(stateNames,
                    owner.toString(), startDate, endDate);
        } else if (states.size() == 1) {
            return dao.listLogFilesByOwnerAndDateAndState(states.iterator().next().name(), owner.toString(), startDate, endDate);
        } else {
            //May be the case that it isn't possible to hold multiple database cursors open at once:
            //java.sql.SQLException: invalid cursor state: identified cursor is not open
            //Therefore drain each iterator fully. (Alternatively could implement smart iterator that doesn't issue
            //next query until prior query exhausted...)
            Iterator<LogFile> accumulator = Iterators.emptyIterator();
            for (LogFileState state : states) {
                ArrayList <LogFile> fetched = new ArrayList<>();
                Iterators.addAll(fetched, dao.listLogFilesByOwnerAndDateAndState(state.name(), owner.toString(), startDate, endDate));
                accumulator = (accumulator == null) ? fetched.iterator() : Iterators.concat(accumulator, fetched.iterator());
            }
            final Iterator<LogFile> result = accumulator;
            return new ResultIterator<LogFile>() {
                @Override
                public void close() {
                    // no op for this one.
                }

                @Override
                public boolean hasNext() {
                    return result.hasNext();
                }

                @Override
                public LogFile next() {
                    return result.next();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    public static ResultIterator<LogFile> listLogFilesByDateAndState(JDBILogFileDAO dao, Set <String> states, DateTime startDate, DateTime endDate) {
        if (dao instanceof JDBILogFileDAOWithArraySupport) {
            return ((JDBILogFileDAOWithArraySupport) dao).listLogFilesByDateAndState(states, startDate, endDate);
        } else if (states.size() == 1) {
            return dao.listLogFilesByDateAndState(states.iterator().next(), startDate, endDate);
        } else {
            //May be the case that it isn't possible to hold multiple database cursors open at once:
            //java.sql.SQLException: invalid cursor state: identified cursor is not open
            //Therefore drain each iterator fully. (Alternatively could implement smart iterator that doesn't issue
            //next query until prior query exhausted...)
            Iterator<LogFile> accumulator = Iterators.emptyIterator();
            for (String state : states) {
                ArrayList <LogFile> fetched = new ArrayList<>();
                Iterators.addAll(fetched, dao.listLogFilesByDateAndState(state, startDate, endDate));
                accumulator = (accumulator == null) ? fetched.iterator() : Iterators.concat(accumulator, fetched.iterator());
            }
            final Iterator<LogFile> result = accumulator;
            return new ResultIterator<LogFile>() {
                @Override
                public void close() {
                    // no op for this one.
                }

                @Override
                public boolean hasNext() {
                    return result.hasNext();
                }

                @Override
                public LogFile next() {
                    return result.next();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    public static ResultIterator<LogFile> listLogFilesByDate(JDBILogFileDAO dao, DateTime startDate, DateTime endDate) {
        return dao.listLogFilesByDate(startDate, endDate);
    }


    public static int count(JDBILogFileDAO dao, Set<String> states) {
        if (dao instanceof JDBILogFileDAOWithArraySupport) {
            return ((JDBILogFileDAOWithArraySupport)dao).count(states);
        } else if (states.size() == 1) {
            return dao.count(states.iterator().next());
        } else {
            int result = 0;
            for (String state : states) {
                result += dao.count(state);
            }
            return result;
        }
    }

    public static int count(JDBILogFileDAO dao, Set<String> states, DateTime startDate, DateTime endDate) {
        if (dao instanceof JDBILogFileDAOWithArraySupport) {
            return ((JDBILogFileDAOWithArraySupport)dao).count(states, startDate, endDate);
        } else if (states.size() == 1) {
            return dao.count(states.iterator().next(), startDate, endDate);
        } else {
            int result = 0;
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
