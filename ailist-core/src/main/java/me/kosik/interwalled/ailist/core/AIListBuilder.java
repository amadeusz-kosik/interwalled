package me.kosik.interwalled.ailist.core;

import me.kosik.interwalled.ailist.core.model.Configuration;
import me.kosik.interwalled.ailist.core.model.Interval;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


public class AIListBuilder<T> implements Serializable {

    private final Configuration config;

    public AIListBuilder(final Configuration config) {
        this.config = config;
    }

    public List<AIList<T>> build(ArrayList<Interval<T>> intervals) {
        assert config.intervalsCountToCheckLookahead() >= config.intervalsCountToTriggerExtraction();
        assert config.maximumComponentsCount() == 1 || (config.intervalsCountToCheckLookahead() > 0);

        // Edge case: at start of the algorithm assign everything to a single component.
        if (intervals.size() <= config.maximumComponentSize() || config.maximumComponentsCount() == 1) {
            AIList<T> result = new AIList<>(intervals);
            return List.of(result);
        }

        List<AIList<T>> results = new LinkedList<>();

        while(results.size() - 1 < config.maximumComponentsCount()) {
            ArrayList<Interval<T>> newComponent = new ArrayList<>();
            ArrayList<Interval<T>> leftovers = new ArrayList<>();

            while(!intervals.isEmpty() && newComponent.size() < config.maximumComponentSize()) {
                Interval<T> nextInterval = intervals.get(0);
                intervals.remove(0);

                boolean coverage = computeCoverage(nextInterval.to(), intervals);

                if(! coverage)
                    newComponent.add(nextInterval);
                else
                    leftovers.add(nextInterval);
            }

            if(!intervals.isEmpty())
                leftovers.addAll(intervals);

            intervals = leftovers;
            results.add(new AIList<>(newComponent));
        }

        return results;
    }

    private boolean computeCoverage(final long intervalTo, final ArrayList<Interval<T>> intervals) {
        int lookaheadCoverage = 0;

        // Count interval's coverage: how many further intervals are "covered" by the current one's length.
        for(int lookaheadIndex = 0; lookaheadIndex < config.intervalsCountToCheckLookahead(); ++ lookaheadIndex) {
            // Guard against going outside the intervals' list.
            //  Break if all intervals are already visited.
            if (lookaheadIndex >= intervals.size())
                break;

            // If current interval is reaching further than the checked
            //  one, increment coverage
            if (intervals.get(lookaheadIndex).to() <= intervalTo)
                lookaheadCoverage ++;

            // If enough intervals are already covered, skip browsing the rest.
            if (lookaheadCoverage >= config.intervalsCountToTriggerExtraction())
                break;
        }

        return lookaheadCoverage < config.intervalsCountToTriggerExtraction();
    }
}
