package io.ifar.skidroad.tracking;

public enum LogFileState {
    WRITING,WRITTEN,WRITE_ERROR,
    PREPARING,PREPARED,PREP_ERROR,
    UPLOADING,UPLOADED,UPLOAD_ERROR,
}
