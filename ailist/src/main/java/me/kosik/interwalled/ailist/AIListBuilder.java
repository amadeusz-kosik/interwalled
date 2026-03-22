package me.kosik.interwalled.ailist;

import me.kosik.interwalled.ailist.model.AIListConfiguration;
import me.kosik.interwalled.ailist.model.Interval;
import me.kosik.interwalled.ailist.model.IntervalComparator;
import me.kosik.interwalled.ailist.model.Intervals;

import java.io.Serializable;
import java.util.ArrayList;

public class AIListBuilder implements Serializable {

    private final AIListConfiguration config;
    private final ArrayList<Interval> intervals;

    public AIListBuilder(final AIListConfiguration config) {
        this.config    = config;
        this.intervals = new ArrayList<>();
    }

    public AIListBuilder(final AIListConfiguration config, final ArrayList<Interval> intervals) {
        this.config    = config;
        this.intervals = intervals;
    }

    public AIList build() {
        assert config.intervalsCountToCheckLookahead() >= config.intervalsCountToTriggerExtraction();
        assert config.maximumComponentsCount() == 1 || (config.intervalsCountToCheckLookahead() > 0);

        if(! config.isInputDataSorted()) {
            intervals.sort(IntervalComparator.comparing());
        }

        int componentsCount = 0;
        ArrayList<Integer> componentsLengths = new ArrayList<>();
        ArrayList<Integer> componentsStartIndexes = new ArrayList<>();

        if (intervals.size() <= config.maximumComponentSize() || config.maximumComponentsCount() == 1) {
            // Edge case: at start of the algorithm assign everything to a single component.
            componentsCount = 1;
            componentsLengths.add(intervals.size());
            componentsStartIndexes.add(0);
        } else {
            // Decompose lists while:
            //  1/ max component number is not exceeded
            //  2/ number of intervals left is big enough
            //  3/ it is worth to decompose

            int lastAssignedIndex = -1;

            for(int componentIndex = 0; componentIndex < config.maximumComponentsCount() - 1; ++ componentIndex) {
                // If the number of intervals left is smaller than expected minimal component size, then break.
                if(lastAssignedIndex  >= (intervals.size() - config.maximumComponentSize())) {
                    break;
                }

                int currentComponentStartIndex = lastAssignedIndex + 1;
                int currentComponentLength = 0;

                ArrayList<Interval> extractedIntervals = new ArrayList<>();

                for(int currentIntervalIndex = currentComponentStartIndex; currentIntervalIndex < intervals.size(); ) {
                    final Interval currentInterval = intervals.get(currentIntervalIndex);

                    boolean coverage = computeCoverage(currentComponentStartIndex, currentIntervalIndex, intervals);

                    if(! coverage) {
                        // Move the current interval to the extracted ones.
                        extractedIntervals.add(currentInterval);
                        intervals.remove(currentIntervalIndex);
                    } else {
                        lastAssignedIndex = currentIntervalIndex;

                        // Move the index right (skip).
                        ++ currentIntervalIndex;
                        ++ currentComponentLength;
                    }
                }

                // Save new component
                ++ componentsCount;
                componentsStartIndexes.add(currentComponentStartIndex);
                componentsLengths.add(currentComponentLength);

                // Re-add extracted intervals back to the original list.
                intervals.addAll(extractedIntervals);
            }

            // Put remaining elements to the last component
            if(componentsLengths.stream().mapToLong(Integer::longValue).sum() < intervals.size()) {
                int lastComponentStartIndex = (int) componentsLengths.stream().mapToLong(Integer::longValue).sum();

                ++ componentsCount;

                componentsStartIndexes.add(lastComponentStartIndex);
                componentsLengths.add(intervals.size() - lastComponentStartIndex);
            }
        }

        long[] componentsMaxEnds = new long[intervals.size()];

        for (int i = 0; i < componentsCount; i ++) {
            final int componentStart = componentsStartIndexes.get(i);
            final int componentEnd   = componentStart + componentsLengths.get(i) - 1;

            long maxEnd = intervals.get(componentStart).to();
            componentsMaxEnds[componentStart] = maxEnd;

            for (int j = componentStart + 1; j <= componentEnd; j ++) {
                maxEnd = Math.max(intervals.get(j).to(), maxEnd);
                componentsMaxEnds[j] = maxEnd;
            }
        }

        return new AIList(
            new Intervals(intervals),
            componentsCount,
            componentsLengths.stream().mapToInt(Integer::valueOf).toArray(),
            componentsStartIndexes.stream().mapToInt(Integer::valueOf).toArray(),
            componentsMaxEnds
        );
    }

    private boolean computeCoverage(
            final int componentStartIndex,
            final int currentIntervalIndex,
            final ArrayList<Interval> intervals
    ) {
        if(config.checkLookbehindCoverage()) {
            // Check all intervals from (last - intervalsCountToCheckLookahead) to last
            int _from = Math.max(componentStartIndex, currentIntervalIndex - config.intervalsCountToCheckLookahead());
            int _to = currentIntervalIndex;

            int lookbehindCoverage = 0;

            for(int lookbehindIndex = _from; lookbehindIndex < _to; lookbehindIndex += 1) {
                if(intervals.get(lookbehindIndex).to() >= intervals.get(currentIntervalIndex).to()) {
                    lookbehindCoverage += 1;
                }
            }

            if(lookbehindCoverage >= config.intervalsCountToTriggerExtraction())
                return false;
        }

        int lookaheadCoverage = 0;

        // Count interval's coverage: how many further intervals are "covered" by the current one's length.
        for(int lookaheadOffset = 1; lookaheadOffset <= config.intervalsCountToCheckLookahead(); ++ lookaheadOffset) {
            int lookaheadIndex = lookaheadOffset + currentIntervalIndex;

            // Guard against going outside the intervals' list.
            //  Break if all intervals are already visited.
            if (lookaheadIndex >= intervals.size())
                break;

            // If current interval is reaching further than the checked
            //  one, increment coverage
            if (intervals.get(lookaheadIndex).to() <= intervals.get(currentIntervalIndex).to())
                lookaheadCoverage ++;

            // If enough intervals are already covered, skip browsing the rest.
            if (lookaheadCoverage >= config.intervalsCountToTriggerExtraction())
                break;
        }

        return lookaheadCoverage < config.intervalsCountToTriggerExtraction();
    }

    public void put(final Interval interval) {
        intervals.add(interval);
    }
}
