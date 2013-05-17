package io.ifar.skidroad.jdbi;

/**
 *
 */
public class CountByState {

    private final String state;
    private final int count;

    public CountByState(String state, int count) {
        this.state = state;
        this.count = count;
    }

    public String getState() {
        return state;
    }

    public int getCount() {
        return count;
    }
}
