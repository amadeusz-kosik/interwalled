package me.kosik.interwalled.ailist.utils;

import me.kosik.interwalled.ailist.AIList;
import me.kosik.interwalled.ailist.AIListBuilder;
import me.kosik.interwalled.ailist.AIListIterator;
import me.kosik.interwalled.ailist.model.AIListConfiguration;
import me.kosik.interwalled.ailist.model.Interval;
import me.kosik.interwalled.ailist.model.IntervalsPair;

import java.util.ArrayList;
import java.util.List;


public class ListBuilder {

    public static <T> AIList<T> buildAIList(final List<Interval<T>> list) {
        return buildAIList(AIListConfiguration.DEFAULT, list);
    }

    public static <T> AIList<T> buildAIList(final AIListConfiguration configuration, final List<Interval<T>> list) {
        final AIListBuilder<T> aiListBuilder = new AIListBuilder<>(configuration);

        for (final Interval<T> interval : list) {
            aiListBuilder.put(interval);
        }

        return aiListBuilder.build();
    }

    public static <T, U> List<IntervalsPair<T, U>> buildActual(final AIList<T> lhs, final List<Interval<U>> rhs) {
        final List<IntervalsPair<T, U>> result = new ArrayList<>();

        for (final Interval<U> rhsInterval : rhs) {
            AIListIterator<T> resultsIterator = lhs.overlapping(rhsInterval);

            while (resultsIterator.hasNext()) {
                Interval<T> lhsInterval = resultsIterator.next();
                result.add(IntervalsPair.fromIntervals(lhsInterval, rhsInterval));
            }
        }

        result.sort(new IntervalsPairComparator<>());
        return result;
    }

    public static <T, U> List<IntervalsPair<T, U>> buildExpected(final List<Interval<T>> lhs, final List<Interval<U>> rhs) {
        final List<IntervalsPair<T, U>> result = new ArrayList<>();

        for(final Interval<T> lhsInterval : lhs) { for (final Interval<U> rhsInterval : rhs) {
            if (Interval.overlaps(lhsInterval, rhsInterval)) {
                result.add(IntervalsPair.fromIntervals(lhsInterval, rhsInterval));
            }
        }}

        result.sort(new IntervalsPairComparator<>());
        return result;
    }
}

