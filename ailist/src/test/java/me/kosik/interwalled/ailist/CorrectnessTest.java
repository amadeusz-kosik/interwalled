package me.kosik.interwalled.ailist;

import me.kosik.interwalled.ailist.model.Interval;
import me.kosik.interwalled.ailist.model.IntervalsPair;
import me.kosik.interwalled.ailist.generator.DataGenerators;
import me.kosik.interwalled.ailist.utils.ListBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.util.List;


public class CorrectnessTest {

    /* Each correctness test asserts that list stores exactly the same elements as they
     *  were put into the list: no duplication, no data loss. */

    @Test
    void noOverlappingIntervals() {
        final List<Interval<String>> lhs = DataGenerators.consecutiveIntervals(100);
        final List<Interval<String>> rhs = DataGenerators.consecutiveIntervals(200, 100);

        final AIList<String> aiList = ListBuilder.buildAIList(lhs);

        final List<IntervalsPair<String, String>> actual   = ListBuilder.buildActual(aiList, rhs);
        final List<IntervalsPair<String, String>> expected = ListBuilder.buildExpected(lhs, rhs);

        assertIterableEquals(expected, actual);
    }

    @Test
    void oneToOneIntervals() {
        final List<Interval<String>> lhs = DataGenerators.consecutiveIntervals(100);
        final List<Interval<String>> rhs = DataGenerators.consecutiveIntervals(100);

        final AIList<String> aiList = ListBuilder.buildAIList(lhs);

        final List<IntervalsPair<String, String>> actual   = ListBuilder.buildActual(aiList, rhs);
        final List<IntervalsPair<String, String>> expected = ListBuilder.buildExpected(lhs, rhs);

        assertIterableEquals(expected, actual);
    }

    @Test
    void allToOneIntervals() {
        final List<Interval<String>> lhs = DataGenerators.consecutiveIntervals(100);
        final List<Interval<String>> rhs = DataGenerators.consecutiveIntervals(0, 100, 1);

        final AIList<String> aiList = ListBuilder.buildAIList(lhs);

        final List<IntervalsPair<String, String>> actual   = ListBuilder.buildActual(aiList, rhs);
        final List<IntervalsPair<String, String>> expected = ListBuilder.buildExpected(lhs, rhs);

        assertIterableEquals(expected, actual);
    }

    @Test
    void oneToAllIntervals() {
        final List<Interval<String>> lhs = DataGenerators.consecutiveIntervals(0, 101, 1);
        final List<Interval<String>> rhs = DataGenerators.consecutiveIntervals(100);

        final AIList<String> aiList = ListBuilder.buildAIList(lhs);

        final List<IntervalsPair<String, String>> actual   = ListBuilder.buildActual(aiList, rhs);
        final List<IntervalsPair<String, String>> expected = ListBuilder.buildExpected(lhs, rhs);

        assertIterableEquals(expected, actual);
    }
}

