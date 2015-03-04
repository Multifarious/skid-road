package io.ifar.skidroad.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.base.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.AbstractLogFileTracker;
import io.ifar.skidroad.tracking.LogFileState;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.skife.jdbi.v2.ResultIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static io.ifar.skidroad.tracking.LogFileState.WRITING;

/**
 *
 */
public class DynamoDBLogFileTracker extends AbstractLogFileTracker {

    public static final Joiner SLASH_JOINER = Joiner.on("/");
    private final AmazonDynamoDB ddb;

    private static final DateTimeFormatter ISO_DT = ISODateTimeFormat.basicDateTime()
                                                                     .withZoneUTC();
    private final String uriRoot;
    private final String logFilesTable;
    private final String talliesTable;

    public DynamoDBLogFileTracker(@Nonnull URI localUri, @Nonnull String talliesTable,
                                  @Nonnull String logFilesTable, @Nonnull AmazonDynamoDB ddb)
    {
        super(localUri);
        String uriRoot = localUri.toASCIIString();
        // We'll join on slashes.
        if (uriRoot.endsWith("/")) {
            uriRoot = uriRoot.substring(0,uriRoot.length()-1);
        }
        this.uriRoot = uriRoot;
        this.ddb = ddb;
        this.talliesTable = talliesTable;
        this.logFilesTable = logFilesTable;
    }

    private Integer nextIndex(String rollingCohort) {
        Preconditions.checkNotNull(rollingCohort);
        UpdateItemRequest req = new UpdateItemRequest()
                .withTableName(talliesTable)
                .withKey(makeEntry("rolling_cohort", rollingCohort), makeEntry("node_id", uriRoot))
                .withAttributeUpdates(ImmutableMap.of("tally",
                        new AttributeValueUpdate(new AttributeValue().withN("1"), AttributeAction.ADD)))
                .withReturnValues(ReturnValue.UPDATED_NEW);
        UpdateItemResult result = ddb.updateItem(req);
        return Integer.valueOf(result.getAttributes().get("tally").getN());
    }

    @Override
    protected int recordStateChange(LogFile logFile) {
        Preconditions.checkNotNull(logFile);
        Preconditions.checkNotNull(logFile.getState());
        DateTime now = new DateTime(DateTimeZone.UTC);
        ddb.updateItem(updateRequestFor(logFile, "state", logFile.getState().name(), ISO_DT.print(now)));
        logFile.setUpdatedAt(now);
        return 1;
    }

    @Override
    protected int updateSize(LogFile logFile) {
        Preconditions.checkNotNull(logFile);
        Preconditions.checkNotNull(logFile.getByteSize());
        DateTime now = new DateTime(DateTimeZone.UTC);
        ddb.updateItem(updateRequestFor(logFile, "bytes", logFile.getByteSize(), ISO_DT.print(now)));
        logFile.setUpdatedAt(now);
        return 1;
    }

    @Override
    public LogFile open(String rollingCohort, String pathPattern, @Nonnull DateTime startTime) {
        Integer serial = nextIndex(rollingCohort);
        Path originPath = Paths.get(String.format(pathPattern, serial));
        DateTime stamp = new DateTime(DateTimeZone.UTC);
        LogFile fresh = new LogFile(rollingCohort, serial, startTime, originPath, null, null,
                null, null, WRITING, localUri, null, stamp, null);
        ImmutableMap.Builder<String, AttributeValue> body = ImmutableMap
                .builder();
        body.put(logFileHashKeyFor(fresh))
            .put(logFileRangeKeyFor(fresh))
            .put("origin_path", new AttributeValue().withS(originPath.toString()))
            .put("owner_uri", new AttributeValue().withS(localUri.toASCIIString()))
            .put("start_time", new AttributeValue().withS(ISO_DT.print(startTime)))
            .put("state", new AttributeValue().withS(WRITING.name()))
            .put("serial", new AttributeValue().withN(String.valueOf(serial)))
            .put("created_at", new AttributeValue().withS(ISO_DT.print(stamp)));

        PutItemRequest req = new PutItemRequest()
                .withTableName(logFilesTable)
                .withItem(body.build());
        ddb.putItem(req);
        return fresh;
    }

    @Override
    public int updatePrepPath(LogFile logFile) {
        Preconditions.checkNotNull(logFile);
        DateTime now = new DateTime();
        ddb.updateItem(updateRequestFor(logFile,
                                        "prep_path", logFile.getPrepPath().toUri().toASCIIString(),
                       ISO_DT.print(now)));
        logFile.setUpdatedAt(now);
        return 1;
    }

    @Override
    public int updateArchiveKey(LogFile logFile) {
        Preconditions.checkNotNull(logFile);
        DateTime now = new DateTime(DateTimeZone.UTC);
        ddb.updateItem(updateRequestFor(logFile,
                "archive_key", logFile.getArchiveKey(),
                                        ISO_DT.print(now)));
        logFile.setUpdatedAt(now);
        return 1;
    }

    @Override
    public int updateArchiveLocation(LogFile logFile) {
        Preconditions.checkNotNull(logFile);
        ddb.updateItem(updateRequestFor(logFile,
                                        "archive_group", logFile.getArchiveGroup(),
                                        "archive_uri", logFile.getArchiveURI().toString()));
        return 1;
    }

    @Override
    public ResultIterator<LogFile> findMine(LogFileState state) {
        Preconditions.checkNotNull(state);
        return iteratorOverResults(new ScanRequest()
                .withTableName(logFilesTable)
                .withScanFilter(ImmutableMap.of("state", new Condition()
                        .withAttributeValueList(new AttributeValue().withS(state.name()))
                        .withComparisonOperator(ComparisonOperator.EQ)))
        );
    }

    private ResultIterator<LogFile> iteratorOverResults(final ScanRequest req) {
        final ScanResult result = ddb.scan(req);
        return new ResultIterator<LogFile>() {

            private Map<String, AttributeValue> lastScannedKey = result.getLastEvaluatedKey();
            private Iterator<Map<String,AttributeValue>> current = result.getItems().iterator();

            @Override
            public void close() {
                // no op
            }

            @Override
            public boolean hasNext() {
                if (current.hasNext()) {
                    return true;
                } else if (lastScannedKey == null) {
                    return false;
                } else {
                    req.setExclusiveStartKey(lastScannedKey);
                    ScanResult result = ddb.scan(req);
                    lastScannedKey = result.getLastEvaluatedKey();
                    current = result.getItems().iterator();
                    return hasNext();
                }
            }

            @Override
            public LogFile next() {
                if (hasNext()) {
                    return toLogFile(current.next());
                }
                throw new NoSuchElementException();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private LogFile toLogFile(Map<String,AttributeValue> item) {
        return new LogFile(toStringOrNull(item, "rolling_cohort"),
                    toIntegerOrNull(item, "serial"),
                    toDateTimeOrNull(item, "start_time"),
                    toPathOrNull(item, "origin_path"),
                    toPathOrNull(item, "prep_path"),
                    toStringOrNull(item, "archive_key"),
                    toUriOrNull(item, "archive_uri"),
                    toStringOrNull(item, "archive_group"),
                    toLogFileStateOrNull(item, "state"),
                    toUriOrNull(item, "owner_uri"),
                    toLongOrNull(item, "bytes"),
                    toDateTimeOrNull(item, "created_at"),
                    toDateTimeOrNull(item, "updated_at")
            );
    }

    private static @Nullable String toStringOrNull(Map<String, AttributeValue> item, String property) {
        return Optional.fromNullable(item.get(property))
                .transform(new Function<AttributeValue, String>() {
                    @Override
                    public String apply(AttributeValue attributeValue) {
                        return attributeValue.getS();
                    }
                }).orNull();
    }

    private static @Nullable Path toPathOrNull(Map<String, AttributeValue> item, String property) {
        return Optional.fromNullable(item.get(property))
                       .transform(new Function<AttributeValue, Path>() {
                           @Override
                           public Path apply(AttributeValue attributeValue) {
                               return Paths.get(attributeValue.getS());
                           }
                       }).orNull();
    }

    private static @Nullable DateTime toDateTimeOrNull(Map<String, AttributeValue> item , String property) {
        return Optional.fromNullable(item.get(property))
                       .transform(new Function<AttributeValue, DateTime>() {
                           @Override
                           public DateTime apply(AttributeValue attributeValue) {
                               return ISO_DT.parseDateTime(attributeValue.getS());
                           }
                       }).orNull();
    }

    private static @Nullable URI toUriOrNull(Map<String, AttributeValue> item , String property) {
        return Optional.fromNullable(item.get(property))
                       .transform(new Function<AttributeValue, URI>() {
                           @Override
                           public URI apply(AttributeValue attributeValue) {
                               try {
                                   return new URI(attributeValue.getS());
                               } catch (URISyntaxException use) {
                                   throw Throwables.propagate(use);
                               }
                           }
                       }).orNull();
    }

    private static @Nullable LogFileState toLogFileStateOrNull(Map<String, AttributeValue> item, String property) {
        return Optional.fromNullable(item.get(property))
                       .transform(new Function<AttributeValue, LogFileState>() {
                           @Override
                           public LogFileState apply(AttributeValue attributeValue) {
                               return LogFileState.valueOf(attributeValue.getS());
                           }
                       }).orNull();
    }

    private static @Nullable Integer toIntegerOrNull(Map<String, AttributeValue> item, String property) {
        return Optional.fromNullable(item.get(property))
                       .transform(new Function<AttributeValue, Integer>() {
                           @Override
                           public Integer apply(AttributeValue attributeValue) {
                               return Integer.valueOf(attributeValue.getN());
                           }
                       }).orNull();
    }

    private static @Nullable Long toLongOrNull(Map<String, AttributeValue> item, String property) {
        return Optional.fromNullable(item.get(property))
                       .transform(new Function<AttributeValue, Long>() {
                           @Override
                           public Long apply(AttributeValue attributeValue) {
                               return Long.valueOf(attributeValue.getN());
                           }
                       }).orNull();
    }

    @Override
    public ResultIterator<LogFile> findMine(Set<LogFileState> states) {
        Preconditions.checkNotNull(states);
        return iteratorOverResults(new ScanRequest()
                                           .withTableName(logFilesTable)
                                           .withScanFilter(ImmutableMap.of("state", new Condition()
                                                   .withAttributeValueList(new AttributeValue().withSS(
                                                           Sets.newHashSet(Iterators.transform(states.iterator(),
                                                                                               new Function<LogFileState, String>() {
                                                                                                   @Nullable
                                                                                                   @Override
                                                                                                   public String apply(LogFileState input) {
                                                                                                       return input.name();
                                                                                                   }
                                                                                               })
                                                           )))
                                                   .withComparisonOperator(ComparisonOperator.IN)))
        );
    }

    @Override
    public ResultIterator<LogFile> findMine(LogFileState state, DateTime start, DateTime end) {
        Preconditions.checkNotNull(state);
        return iteratorOverResults(new ScanRequest()
                                           .withTableName(logFilesTable)
                                           .withScanFilter(ImmutableMap.<String, Condition>builder()
                                                                       .put("state", new Condition()
                                                                               .withAttributeValueList(new AttributeValue().withS(state.name()))
                                                                               .withComparisonOperator(ComparisonOperator.EQ))
                                                                       .put("start_date", new Condition()
                                                                               .withAttributeValueList(new AttributeValue().withS(ISO_DT.print(start)))
                                                                               .withComparisonOperator(ComparisonOperator.GE))
                                                                       .put("start_date", new Condition()
                                                                               .withAttributeValueList(new AttributeValue().withS(ISO_DT.print(end)))
                                                                               .withComparisonOperator(ComparisonOperator.LE))
                                                                       .build())
        );
    }

    @Override
    public ResultIterator<LogFile> findMine(Set<LogFileState> states, DateTime start, DateTime end) {
        Preconditions.checkNotNull(states);
        return iteratorOverResults(new ScanRequest()
                                           .withTableName(logFilesTable)
                                           .withScanFilter(ImmutableMap.<String, Condition>builder()
                                                                       .put("state", new Condition()
                                                                               .withAttributeValueList(new AttributeValue().withSS(
                                                                                       Sets.newHashSet(Iterators.transform(states.iterator(),
                                                                                                                           new Function<LogFileState, String>() {
                                                                                                                               @Nullable
                                                                                                                               @Override
                                                                                                                               public String apply(LogFileState input) {
                                                                                                                                   return input.name();
                                                                                                                               }
                                                                                                                           })
                                                                                       )))
                                                                               .withComparisonOperator(ComparisonOperator.IN))
                                                                       .put("start_date", new Condition()
                                                                               .withAttributeValueList(new AttributeValue().withS(ISO_DT.print(start)))
                                                                               .withComparisonOperator(ComparisonOperator.GE))
                                                                       .put("start_date", new Condition()
                                                                               .withAttributeValueList(new AttributeValue().withS(ISO_DT.print(end)))
                                                                               .withComparisonOperator(ComparisonOperator.LE))
                                                                       .build())
        );
    }

    @Override
    public int getCount(LogFileState state) {
        ScanRequest req = new ScanRequest()
                .withTableName(logFilesTable)
                .withScanFilter(ImmutableMap.of("state", new Condition()
                        .withAttributeValueList(new AttributeValue().withS(state.name()))
                        .withComparisonOperator(ComparisonOperator.EQ)))
                .withSelect(Select.COUNT);
        ScanResult result = ddb.scan(req);
        return result.getCount();
    }

    @Override
    public int getCount(Set<LogFileState> states) {
        Preconditions.checkNotNull(states);
        ScanRequest req = new ScanRequest()
                .withTableName(logFilesTable)
                .withScanFilter(ImmutableMap.of("state", new Condition()
                        .withAttributeValueList(new AttributeValue().withSS(
                                Sets.newHashSet(Iterators.transform(states.iterator(),
                                                                    new Function<LogFileState, String>() {
                                                                        @Nullable
                                                                        @Override
                                                                        public String apply(LogFileState input) {
                                                                            return input.name();
                                                                        }
                                                                    })
                                )))
                        .withComparisonOperator(ComparisonOperator.IN)))
                .withSelect(Select.COUNT);
        ScanResult result = ddb.scan(req);
        return result.getCount();
    }

    private UpdateItemRequest updateRequestFor(LogFile logFile, String field, String value, String now) {
        return new UpdateItemRequest()
                .withTableName(logFilesTable)
                .withKey(logFileHashKeyFor(logFile), logFileRangeKeyFor(logFile))
                .withAttributeUpdates(ImmutableMap.of(
                        field, new AttributeValueUpdate(new AttributeValue(value), AttributeAction.PUT),
                        "updated_at", new AttributeValueUpdate()
                                .withValue(new AttributeValue(now))
                ));
    }

    private UpdateItemRequest updateRequestFor(LogFile logFile, String field, Number value, String now) {
        return new UpdateItemRequest()
                .withTableName(logFilesTable)
                .withKey(logFileHashKeyFor(logFile), logFileRangeKeyFor(logFile))
                .withAttributeUpdates(ImmutableMap.of(
                        field, new AttributeValueUpdate(new AttributeValue().withN(String.valueOf(value)), AttributeAction.PUT),
                        "updated_at", new AttributeValueUpdate()
                                .withValue(new AttributeValue(now))
                ));
    }


    private UpdateItemRequest updateRequestFor(final LogFile logFile, String field1, String value1, String field2, String value2) {
        return new UpdateItemRequest()
                .withTableName(logFilesTable)
                .withKey(logFileHashKeyFor(logFile), logFileRangeKeyFor(logFile))
                .withAttributeUpdates(ImmutableMap.of(
                        field1, new AttributeValueUpdate(new AttributeValue(value1), AttributeAction.PUT),
                        field2, new AttributeValueUpdate(new AttributeValue(value2), AttributeAction.PUT),
                        "updated_at", new AttributeValueUpdate()
                                .withValue(new AttributeValue(now()))
                ));
    }

    private Map.Entry<String, AttributeValue> logFileHashKeyFor(final LogFile logFile) {
        return makeEntry("rolling_cohort", logFile.getRollingCohort());
    }

    private Map.Entry<String,AttributeValue> logFileRangeKeyFor(LogFile logFile) {
        return makeEntry("file_id", SLASH_JOINER.join(uriRoot, logFile.getRollingCohort(), String.valueOf(logFile.getSerial())));
    }

    private Map.Entry<String, AttributeValue> makeEntry(final String field, final String value) {
        final AttributeValue v = new AttributeValue().withS(value);
        return  new Map.Entry<String, AttributeValue>() {
            @Override
            public String getKey() {
                return field;
            }

            @Override
            public AttributeValue getValue() {
                return v;
            }

            @Override
            public AttributeValue setValue(AttributeValue value) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private String now() {
        return ISO_DT.print(System.currentTimeMillis());
    }
}
