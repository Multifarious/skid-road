package io.ifar.skidroad.rolling;

import com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * Tests for {@link io.ifar.skidroad.rolling.BasicFileRollingScheme}.
 */
@RunWith(Parameterized.class)
public class BasicFileRollingSchemeTest {

    private final Duration duration;
    private final boolean ok;

    @Parameterized.Parameters
    public static List<Object[]> parameters() {
        return ImmutableList.<Object[]>of(
                new Object[] {Duration.standardDays(1), true},
                new Object[] {Duration.standardDays(1).plus(Duration.standardHours(1)), false},
                new Object[] {Duration.standardDays(2).plus(Duration.standardMinutes(30)), false},
                new Object[] {Duration.standardHours(1), true},
                new Object[] {Duration.standardHours(1).plus(Duration.standardMinutes(30)), false},
                new Object[] {Duration.standardHours(2), true},
                new Object[] {Duration.standardHours(3), true},
                new Object[] {Duration.standardMinutes(5), true},
                new Object[] {Duration.standardMinutes(10), true},
                new Object[] {Duration.standardMinutes(12), true},
                new Object[] {Duration.standardMinutes(15), true},
                new Object[] {Duration.standardMinutes(20), true},
                new Object[] {Duration.standardMinutes(30), true},
                new Object[] {Duration.standardMinutes(45), false},
                new Object[] {Duration.standardMinutes(75), false}
        );
    }

    public BasicFileRollingSchemeTest(Duration duration, boolean ok) {
        this.duration = duration;
        this.ok = ok;
    }

    @Test
    public void smokeTestDuration() {
        try {
            new BasicFileRollingScheme("","","",0,duration);
            if (!ok) {
                Assert.fail(String.format("Expected duration %s to fail.",duration));
            }
        } catch (IllegalArgumentException iae) {
            if (ok) {
                Assert.fail(String.format("Expected duration %s to pass.",duration));
            }
        }
    }

}
