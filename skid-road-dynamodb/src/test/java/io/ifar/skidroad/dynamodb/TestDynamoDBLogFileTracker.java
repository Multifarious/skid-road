package io.ifar.skidroad.dynamodb;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.google.common.collect.Iterators;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileState;
import org.apache.commons.lang.RandomStringUtils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * "Unit" test that uses DynamoDBLocal from AWS for testing.
 *
 * See <a href="http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html">DynamoDBLocal</a>
 * for more information.
 *
 * Download and then start:
 *
 * <pre>
 *     cd WHERE_YOU_UNPACKED_IT
 *     java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -inMemory
 * </pre>
 */
public class TestDynamoDBLogFileTracker {


    private AmazonDynamoDB ddb;
    private DynamoDBLogFileTracker tracker;
    String rollingCohort = RandomStringUtils.randomAlphabetic(20);
    String pathPattern = RandomStringUtils.randomAlphanumeric(50) + "-%d";


    private String talliesTable = String.format("sr_tallies_%s", RandomStringUtils.randomAlphanumeric(10));
    private String logFilesTable = String.format("sr_tallies_%s", RandomStringUtils.randomAlphanumeric(10));

    @Before
    public void setup() throws URISyntaxException {
        ddb = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain());
        ddb.setEndpoint("http://localhost:8000");
        DynamoDBMigrations.createTalliesTable(talliesTable, ddb, 1, 1);
        DynamoDBMigrations.createLogFilesTable(logFilesTable, ddb, 1, 1);
        tracker = new DynamoDBLogFileTracker(new URI("http://local/test"), talliesTable, logFilesTable, ddb);
        tracker.start();
    }

    @After
    public void teardown() {
        tracker.stop();
        ddb.shutdown();
    }

    @Test
    public void increasingSerialNumbers() {
        for (int i =0; i < 100; ++i) {
            LogFile x = tracker.open(rollingCohort, pathPattern, new DateTime());
            Assert.assertEquals(i + 1, x.getSerial().intValue());
        }
    }

    @Test
    public void happyPath() {
        LogFile lf = tracker.open(rollingCohort, pathPattern, new DateTime());

        Assert.assertEquals(LogFileState.WRITING, lf.getState());
        Assert.assertTrue(Iterators.contains(tracker.findMine(LogFileState.WRITING), lf));

        int u = tracker.writeError(lf);
        Assert.assertEquals(1,u);
        Assert.assertEquals(LogFileState.WRITE_ERROR, lf.getState());
        LogFile[] files = Iterators.toArray(tracker.findMine(LogFileState.WRITE_ERROR), LogFile.class);
        Assert.assertTrue(Iterators.contains(tracker.findMine(LogFileState.WRITE_ERROR), lf));

        lf.setByteSize(System.currentTimeMillis());
        u = tracker.written(lf);
        Assert.assertEquals(1,u);
        Assert.assertEquals(LogFileState.WRITTEN, lf.getState());
        Assert.assertTrue(Iterators.contains(tracker.findMine(LogFileState.WRITTEN), lf));

        u = tracker.preparing(lf);
        Assert.assertEquals(1,u);
        Assert.assertEquals(LogFileState.PREPARING, lf.getState());
        Assert.assertTrue(Iterators.contains(tracker.findMine(LogFileState.PREPARING), lf));

        u = tracker.prepError(lf);
        Assert.assertEquals(1, u);
        Assert.assertEquals(LogFileState.PREP_ERROR, lf.getState());
        Assert.assertTrue(Iterators.contains(tracker.findMine(LogFileState.PREP_ERROR), lf));

        u = tracker.preparing(lf);
        Assert.assertEquals(1,u);
        Assert.assertEquals(LogFileState.PREPARING, lf.getState());
        Assert.assertTrue(Iterators.contains(tracker.findMine(LogFileState.PREPARING), lf));

        u = tracker.prepared(lf);
        Assert.assertEquals(1,u);
        Assert.assertEquals(LogFileState.PREPARED, lf.getState());
        Assert.assertTrue(Iterators.contains(tracker.findMine(LogFileState.PREPARED), lf));

        u = tracker.uploading(lf);
        Assert.assertEquals(1,u);
        Assert.assertEquals(LogFileState.UPLOADING, lf.getState());
        Assert.assertTrue(Iterators.contains(tracker.findMine(LogFileState.UPLOADING), lf));

        u = tracker.uploadError(lf);
        Assert.assertEquals(1,u);
        Assert.assertEquals(LogFileState.UPLOAD_ERROR, lf.getState());
        Assert.assertTrue(Iterators.contains(tracker.findMine(LogFileState.UPLOAD_ERROR), lf));

        u = tracker.uploaded(lf);
        Assert.assertEquals(1,u);
        Assert.assertEquals(LogFileState.UPLOADED, lf.getState());
        Assert.assertTrue(Iterators.contains(tracker.findMine(LogFileState.UPLOADED), lf));

    }
}
