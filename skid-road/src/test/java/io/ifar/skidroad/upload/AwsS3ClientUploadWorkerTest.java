package io.ifar.skidroad.upload;

import io.ifar.skidroad.LogFile;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.*;

public class AwsS3ClientUploadWorkerTest {

    @Test
    public void testDetermineArchiveURI() throws URISyntaxException {
        LogFile logFile = new LogFile();
        logFile.setStartTime(new DateTime(2013,10,7,21,33,57, DateTimeZone.UTC));
        logFile.setPrepPath(new File("/var/skid_road_logs/my_project_2013-10-07T21-33_1.gz.aes-sic").toPath());
        AwsS3ClientUploadWorker worker = new AwsS3ClientUploadWorker(logFile, null, new URI("s3://my_bucket/prefix"), null);
        assertEquals(new URI("s3://my_bucket/prefix/2013/10/07/my_project_2013-10-07T21-33_1.gz.aes-sic"), worker.determineArchiveURI(logFile));
    }

    @Test
    public void testDetermineArchiveGroup() {
        LogFile logFile = new LogFile();
        logFile.setStartTime(new DateTime(2013,10,7,21,33,57, DateTimeZone.UTC));
        AwsS3ClientUploadWorker worker = new AwsS3ClientUploadWorker(logFile, null, null, null);
        assertEquals("20131007", worker.determineArchiveGroup(logFile));
    }
}
