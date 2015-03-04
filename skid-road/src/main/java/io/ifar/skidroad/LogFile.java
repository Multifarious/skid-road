package io.ifar.skidroad;

import com.google.common.base.Joiner;
import io.ifar.skidroad.tracking.LogFileState;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.net.URI;
import java.nio.file.Path;

/**
 * Database representation of a log file.
 */
public class LogFile {
    private String rollingCohort;
    private Integer serial;
    private DateTime startTime;
    private Path originPath;
    private Path prepPath;
    private String archiveKey;
    private URI archiveURI;
    private String archiveGroup;
    private LogFileState state;
    private URI ownerURI;
    private Long byteSize;
    private DateTime createdAt;
    private DateTime updatedAt;

    /**
     *
     * @param rollingCohort e.g. hour the log lines belong to
     * @param serial unique serial number within rollingCohort
     * @param startTime the starting time for items in the log file.
     * @param originPath local path on machine creating log file
     * @param archiveKey one-time file key used to encrypt contents. archive key itself usually also encrypted with master key.
     * @param archiveURI e.g. S3 path compressed log file uploaded to
     * @param archiveGroup e.g. week number log file belongs to
     * @param state the state of the log file, e.g., {@link LogFileState#WRITING}.
     * @param ownerURI identifies entity responsible for maintaining state
     * @param byteSize the number of bytes in the file (if known)
     * @param createdAt when the record was updated.
     * @param updatedAt when the record was last updated.
     */
    public LogFile(String rollingCohort, Integer serial, DateTime startTime, Path originPath, Path prepPath,
                   String archiveKey, URI archiveURI, String archiveGroup, LogFileState state, URI ownerURI,
                   Long byteSize, DateTime createdAt,  DateTime updatedAt)
    {
        this.archiveGroup = archiveGroup;
        this.archiveKey = archiveKey;
        this.archiveURI = archiveURI;
        this.byteSize = byteSize;
        this.createdAt = createdAt;
        this.rollingCohort = rollingCohort;
        this.serial = serial;
        this.originPath = originPath;
        this.ownerURI = ownerURI;
        this.prepPath = prepPath;
        this.state = state;
        this.startTime = startTime;
        this.updatedAt = updatedAt;
    }

    public LogFile() {}

    public String getArchiveGroup() {
        return archiveGroup;
    }

    public void setArchiveGroup(String archiveGroup) {
        this.archiveGroup = archiveGroup;
    }

    public URI getArchiveURI() {
        return archiveURI;
    }

    public void setArchiveURI(URI archiveURI) {
        this.archiveURI = archiveURI;
    }

    public Long getByteSize() {
        return byteSize;
    }

    public void setByteSize(Long byteSize) {
        this.byteSize = byteSize;
    }

    public DateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(DateTime createdAt) {
        this.createdAt = createdAt;
    }

    public String getRollingCohort() {
        return rollingCohort;
    }

    public void setRollingCohort(String rollingCohort) {
        this.rollingCohort = rollingCohort;
    }

    public Integer getSerial() {
        return serial;
    }

    public void setSerial(Integer serial) {
        this.serial = serial;
    }

    public Path getOriginPath() {
        return originPath;
    }

    public void setOriginPath(Path originPath) {
        this.originPath = originPath;
    }

    public URI getOwnerURI() {
        return ownerURI;
    }

    public void setOwnerURI(URI ownerURI) {
        this.ownerURI = ownerURI;
    }

    public LogFileState getState() {
        return state;
    }

    public void setState(LogFileState state) {
        this.state = state;
    }

    public DateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(DateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

    public void setPrepPath(Path prepPath) {
        this.prepPath = prepPath;
    }

    public Path getPrepPath() {
        return prepPath;
    }

    public String getArchiveKey() {
        return archiveKey;
    }

    public void setArchiveKey(String archiveKey) {
        this.archiveKey = archiveKey;
    }

    public DateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(DateTime startTime) {
        this.startTime = startTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogFile logFile = (LogFile) o;

        if (archiveGroup != null ? !archiveGroup.equals(logFile.archiveGroup) : logFile.archiveGroup != null)
            return false;
        if (archiveKey != null ? !archiveKey.equals(logFile.archiveKey) : logFile.archiveKey != null) return false;

        if (!nullTolerantUriEquals(archiveURI, logFile.archiveURI)) {
            return false;
        }

        if (byteSize != null ? !byteSize.equals(logFile.byteSize) : logFile.byteSize != null) return false;
        if (!nullTolerantTimezoneIgnorantDateTimeEquals(createdAt, logFile.createdAt)) {
            return false;
        }

        if (originPath != null ? !originPath.equals(logFile.originPath) : logFile.originPath != null) return false;
        if (!nullTolerantUriEquals(ownerURI, logFile.ownerURI)) {
            return false;
        }

        if (prepPath != null ? !prepPath.equals(logFile.prepPath) : logFile.prepPath != null) return false;
        if (rollingCohort != null ? !rollingCohort.equals(logFile.rollingCohort) : logFile.rollingCohort != null)
            return false;
        if (serial != null ? !serial.equals(logFile.serial) : logFile.serial != null) return false;
        if (!nullTolerantTimezoneIgnorantDateTimeEquals(startTime, logFile.startTime)) {
            return false;
        }

        if (state != logFile.state) return false;
        return nullTolerantTimezoneIgnorantDateTimeEquals(updatedAt, logFile.updatedAt);
    }

    private static boolean nullTolerantUriEquals(@Nullable URI left, @Nullable URI right) {
        if (left == null || right == null) {
            return left == right;
        } else {
            return left.toASCIIString().equals(right.toASCIIString());
        }
    }

    private static boolean nullTolerantTimezoneIgnorantDateTimeEquals(@Nullable DateTime left, @Nullable DateTime right) {
        if (left == null || right == null) {
            return left == right;
        } else {
            return left.getMillis() == right.getMillis();
        }
    }

    @Override
    public int hashCode() {
        int result = rollingCohort != null ? rollingCohort.hashCode() : 0;
        result = 31 * result + (serial != null ? serial.hashCode() : 0);
        result = 31 * result + (startTime != null ? startTime.hashCode() : 0);
        result = 31 * result + (originPath != null ? originPath.hashCode() : 0);
        result = 31 * result + (prepPath != null ? prepPath.hashCode() : 0);
        result = 31 * result + (archiveKey != null ? archiveKey.hashCode() : 0);
        result = 31 * result + (archiveURI != null ? archiveURI.hashCode() : 0);
        result = 31 * result + (archiveGroup != null ? archiveGroup.hashCode() : 0);
        result = 31 * result + (state != null ? state.hashCode() : 0);
        result = 31 * result + (ownerURI != null ? ownerURI.hashCode() : 0);
        result = 31 * result + (byteSize != null ? byteSize.hashCode() : 0);
        result = 31 * result + (createdAt != null ? createdAt.hashCode() : 0);
        result = 31 * result + (updatedAt != null ? updatedAt.hashCode() : 0);
        return result;
    }

    private final static Joiner JOINER = Joiner.on(".");
    @Override
    public String toString() {
        return JOINER.join(rollingCohort, serial);
    }

    /**
     * @return unique key for this LogFile
     */
    public String getID() {
        return JOINER.join(rollingCohort, serial);
    }
}
