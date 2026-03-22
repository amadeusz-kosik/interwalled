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

    public static AIList buildAIList(final List<Interval> list) {
        return buildAIList(AIListConfiguration.apply(), list);
    }

    public static  AIList buildAIList(final AIListConfiguration configuration, final List<Interval> list) {
        final AIListBuilder aiListBuilder = new AIListBuilder(configuration);

        for (final Interval interval : list) {
            aiListBuilder.put(interval);
        }

        return aiListBuilder.build();
    }

    public static  List<IntervalsPair> buildActual(final AIList lhs, final List<Interval> rhs) {
        final List<IntervalsPair> result = new ArrayList<>();

        for (final Interval rhsInterval : rhs) {
            AIListIterator resultsIterator = lhs.overlapping(rhsInterval);

            while (resultsIterator.hasNext()) {
                Interval lhsInterval = resultsIterator.next();
                result.add(IntervalsPair.apply(lhsInterval, rhsInterval));
            }
        }

        result.sort(new IntervalsPairComparator());
        return result;
    }

    public static  List<IntervalsPair> buildExpected(final List<Interval> lhs, final List<Interval> rhs) {
        final List<IntervalsPair> result = new ArrayList<>();

        for(final Interval lhsInterval : lhs) { for (final Interval rhsInterval : rhs) {
            if (Interval.overlaps(lhsInterval, rhsInterval)) {
                result.add(IntervalsPair.apply(lhsInterval, rhsInterval));
            }
        }}

        result.sort(new IntervalsPairComparator());
        return result;
    }
}

