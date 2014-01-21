package io.ifar.skidroad.upload;

import io.ifar.skidroad.LogFile;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.*;

public class AwsS3ClientUploadByDirectoryWorkerTest {

    @Test
    public void testDetermineArchiveURI() throws URISyntaxException {
        LogFile logFile = new LogFile();
        logFile.setStartTime(new DateTime(2013,10,7,21,33,57, DateTimeZone.UTC));
        logFile.setPrepPath(new File("/var/skid_road_logs/environment_foo/my_project_2013-10-07T21-33_1.gz.aes-sic").toPath());
        AwsS3ClientUploadByDirectoryWorker worker = new AwsS3ClientUploadByDirectoryWorker(logFile, null, new URI("s3://my_bucket/prefix"), null);
        assertEquals(new URI("s3://my_bucket/prefix/environment_foo/2013/10/07/my_project_2013-10-07T21-33_1.gz.aes-sic"), worker.determineArchiveURI(logFile));
    }

    @Test
    public void testDetermineArchiveGroup() {
        LogFile logFile = new LogFile();
        logFile.setPrepPath(new File("/var/skid_road_logs/environment_foo/my_project_2013-10-07T21-33_1.gz.aes-sic").toPath());
        AwsS3ClientUploadByDirectoryWorker worker = new AwsS3ClientUploadByDirectoryWorker(logFile, null, null, null);
        assertEquals("environment_foo", worker.determineArchiveGroup(logFile));
    }
}
