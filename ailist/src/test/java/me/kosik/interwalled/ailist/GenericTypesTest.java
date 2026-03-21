package me.kosik.interwalled.ailist;

import me.kosik.interwalled.ailist.model.Interval;
import me.kosik.interwalled.ailist.model.IntervalsPair;
import me.kosik.interwalled.ailist.generator.DataGenerators;
import me.kosik.interwalled.ailist.utils.ListBuilder;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;


public class GenericTypesTest {

    @Test
    void integerToString() {
        final List<Interval<Integer>> lhs = integerIntervals();
        final List<Interval<String>>  rhs = stringIntervals();

        final AIList<Integer> aiList = ListBuilder.buildAIList(lhs);

        final List<IntervalsPair<Integer, String>> actual   = ListBuilder.buildActual(aiList, rhs);
        final List<IntervalsPair<Integer, String>> expected = ListBuilder.buildExpected(lhs, rhs);

        assertIterableEquals(expected, actual);
    }

    @Test
    void listToString() {
        final List<Interval<List<Integer>>> lhs = listIntervals();
        final List<Interval<String>>        rhs = stringIntervals();

        final AIList<List<Integer>> aiList = ListBuilder.buildAIList(lhs);

        final List<IntervalsPair<List<Integer>, String>> actual   = ListBuilder.buildActual(aiList, rhs);
        final List<IntervalsPair<List<Integer>, String>> expected = ListBuilder.buildExpected(lhs, rhs);

        assertIterableEquals(expected, actual);
    }

    @Test
    void stringToString() {
        final List<Interval<String>> lhs = stringIntervals();
        final List<Interval<String>> rhs = stringIntervals();

        final AIList<String> aiList = ListBuilder.buildAIList(lhs);

        final List<IntervalsPair<String, String>> actual   = ListBuilder.buildActual(aiList, rhs);
        final List<IntervalsPair<String, String>> expected = ListBuilder.buildExpected(lhs, rhs);

        assertIterableEquals(expected, actual);
    }

    @Test
    void uuidToString() {
        final List<Interval<UUID>>   lhs = uuidIntervals();
        final List<Interval<String>> rhs = stringIntervals();

        final AIList<UUID> aiList = ListBuilder.buildAIList(lhs);

        final List<IntervalsPair<UUID, String>> actual   = ListBuilder.buildActual(aiList, rhs);
        final List<IntervalsPair<UUID, String>> expected = ListBuilder.buildExpected(lhs, rhs);

        assertIterableEquals(expected, actual);
    }

    private List<Interval<Integer>> integerIntervals() {
        return DataGenerators.intervals(1000, i -> new Interval<>(DataGenerators.DEFAULT_KEY, i, i, i));
    }

    private List<Interval<List<Integer>>> listIntervals() {
        return DataGenerators.intervals(1000, i -> new Interval<>(DataGenerators.DEFAULT_KEY, i, i, Arrays.asList(1, 3, 3, 7)));
    }

    private List<Interval<String>> stringIntervals() {
        return DataGenerators.intervals(1000, i -> new Interval<>(DataGenerators.DEFAULT_KEY, i, i, i.toString()));
    }

    private List<Interval<UUID>> uuidIntervals() {
        return DataGenerators.intervals(1000, i -> new Interval<>(DataGenerators.DEFAULT_KEY, i, i, UUID.randomUUID()));
    }
}
