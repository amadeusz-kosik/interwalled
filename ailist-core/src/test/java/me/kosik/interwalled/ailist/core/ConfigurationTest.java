package me.kosik.interwalled.ailist.core;

import me.kosik.interwalled.ailist.core.generator.DataGenerators;
import me.kosik.interwalled.ailist.core.model.Configuration;
import me.kosik.interwalled.ailist.core.model.Interval;
import me.kosik.interwalled.ailist.core.model.IntervalsPair;
import me.kosik.interwalled.ailist.core.benchmark.ListBuilder;
import org.junit.jupiter.api.Test;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;


public class ConfigurationTest {

    /* Each configuration test asserts that list building and iterating work correctly
       no matter what configuration options are used for building. */

    @Test
    void noLookahead() {
        final Configuration configuration = new Configuration(
                1,
                0,
                100000
        );
        final List<Interval> lhs = DataGenerators.consecutiveIntervals(100);
        final List<Interval> rhs = DataGenerators.consecutiveIntervals(100);

        final List<AIList> aiLists = ListBuilder.buildAILists(configuration, lhs);

        final List<IntervalsPair> actual   = ListBuilder.buildActual(aiLists, rhs);
        final List<IntervalsPair> expected = ListBuilder.buildExpected(lhs, rhs);

        assertIterableEquals(expected, actual);
    }
}
