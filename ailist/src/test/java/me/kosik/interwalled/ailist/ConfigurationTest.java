package me.kosik.interwalled.ailist;

import me.kosik.interwalled.ailist.generator.DataGenerators;
import me.kosik.interwalled.ailist.model.AIListConfiguration;
import me.kosik.interwalled.ailist.model.Interval;
import me.kosik.interwalled.ailist.model.IntervalsPair;
import me.kosik.interwalled.ailist.utils.ListBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;


public class ConfigurationTest {

    /* Each configuration test asserts that list building and iterating works correctly
       no matter what configuration options are used for building. */

    @Test
    void noLookahead() {
        final AIListConfiguration configuration = new AIListConfiguration(
                1,
                0,
                0,
                64,
                false,
                false
        );
        final List<Interval<String>> lhs = DataGenerators.consecutiveIntervals(100);
        final List<Interval<String>> rhs = DataGenerators.consecutiveIntervals(100);

        final AIList<String> aiList = ListBuilder.buildAIList(configuration, lhs);

        final List<IntervalsPair<String, String>> actual   = ListBuilder.buildActual(aiList, rhs);
        final List<IntervalsPair<String, String>> expected = ListBuilder.buildExpected(lhs, rhs);

        assertIterableEquals(expected, actual);
    }
}
