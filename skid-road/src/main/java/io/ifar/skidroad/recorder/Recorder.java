package io.ifar.skidroad.recorder;

/**
 * A Recorder is a simple object that can be passed to parts of the system that
 * don't have or want detailed knowledge of the request flow or of Skid Road but need to be able to record data.
 *
 * See {@code RecorderFactory} in skid-road-jersey for helper functions to assemble Recorders
 * in Jersey projects.
 */
public interface Recorder<T> {
    public void record(T item);
}
