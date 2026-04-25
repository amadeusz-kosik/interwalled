package me.kosik.interwalled.ailist.core;

import me.kosik.interwalled.ailist.core.generator.DataGenerators;
import me.kosik.interwalled.ailist.core.model.Interval;
import me.kosik.interwalled.ailist.core.model.IntervalsPair;
import me.kosik.interwalled.ailist.core.benchmark.ListBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.util.List;


public class CorrectnessTest {

    /* Each correctness test asserts that list stores exactly the same elements as they
     *  were put into the list: no duplication, no data loss. */

    @Test
    void noOverlappingIntervals() {
        final List<Interval> lhs = DataGenerators.consecutiveIntervals(100);
        final List<Interval> rhs = DataGenerators.consecutiveIntervals(200, 100);

        final List<AIList> aiLists = ListBuilder.buildAILists(lhs);

        final List<IntervalsPair> actual   = ListBuilder.buildActual(aiLists, rhs);
        final List<IntervalsPair> expected = ListBuilder.buildExpected(lhs, rhs);

        assertIterableEquals(expected, actual);
    }

    @Test
    void oneToOneIntervals() {
        final List<Interval> lhs = DataGenerators.consecutiveIntervals(100);
        final List<Interval> rhs = DataGenerators.consecutiveIntervals(100);

        final List<AIList> aiLists = ListBuilder.buildAILists(lhs);

        final List<IntervalsPair> actual   = ListBuilder.buildActual(aiLists, rhs);
        final List<IntervalsPair> expected = ListBuilder.buildExpected(lhs, rhs);

        assertIterableEquals(expected, actual);
    }

    @Test
    void allToOneIntervals() {
        final List<Interval> lhs = DataGenerators.consecutiveIntervals(100);
        final List<Interval> rhs = DataGenerators.consecutiveIntervals(0, 100, 1);

        final List<AIList> aiLists = ListBuilder.buildAILists(lhs);

        final List<IntervalsPair> actual   = ListBuilder.buildActual(aiLists, rhs);
        final List<IntervalsPair> expected = ListBuilder.buildExpected(lhs, rhs);

        assertIterableEquals(expected, actual);
    }

    @Test
    void oneToAllIntervals() {
        final List<Interval> lhs = DataGenerators.consecutiveIntervals(0, 101, 1);
        final List<Interval> rhs = DataGenerators.consecutiveIntervals(100);

        final List<AIList> aiLists = ListBuilder.buildAILists(lhs);

        final List<IntervalsPair> actual   = ListBuilder.buildActual(aiLists, rhs);
        final List<IntervalsPair> expected = ListBuilder.buildExpected(lhs, rhs);

        assertIterableEquals(expected, actual);
    }
}

