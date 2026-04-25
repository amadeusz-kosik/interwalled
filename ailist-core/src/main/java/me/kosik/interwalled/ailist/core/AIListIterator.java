package me.kosik.interwalled.ailist.core;

import me.kosik.interwalled.ailist.core.model.Interval;
import me.kosik.interwalled.ailist.core.benchmark.ArrayUtils;
import me.kosik.interwalled.ailist.core.benchmark.SearchUtils;

import java.util.Iterator;


public class AIListIterator implements Iterator<Interval>
{
    private final long queryStart;
    private final long queryEnd;

    private final AIList parentAIList;

    private int currentIntervalIndex;

    public AIListIterator(final long queryStart, final long queryEnd, final AIList parentAIList)
    {
        this.queryStart = queryStart;
        this.queryEnd = queryEnd;
        this.parentAIList = parentAIList;
        this.currentIntervalIndex = -1;

        findFirstInterval();
    }

    @Override
    public boolean hasNext() {
        return currentIntervalIndex != -1;
    }

    @Override
    public Interval next() {
        assert(currentIntervalIndex != -1);

        Interval currentReturn = parentAIList.intervals.get(currentIntervalIndex);
        findNextInterval();

        return currentReturn;
    }

    private void findFirstInterval() {
        // Shortcut for completely disjoint intervals.
        //  1/ if components_max < query.start, skip
        if(ArrayUtils.last(parentAIList.maxE) < queryStart) {
            return;
        }

        // Shortcut for completely disjoint intervals.
        //  2/ if first_component.min > query.end, skip
        if(this.parentAIList.intervals.get(0).from() > queryEnd) {
            return;
        }

        // Find the rightmost element satisfying interval.start < queryEnd condition:
        currentIntervalIndex = SearchUtils.findRightmost(parentAIList.intervals, queryEnd);
    }

    public void findNextInterval() {
        // Try picking the next interval in the current component.
        //  In fact, try picking _previous_ interval, as the algorithm goes from right to left.
        //  Do not go beyond the list boundary.
        int _nextIntervalIndex = currentIntervalIndex;

        while(true) {
            _nextIntervalIndex -= 1;

            if(_nextIntervalIndex < 0) {
                // Out of list's boundaries.
                currentIntervalIndex = -1;
                return;
            }

            if(parentAIList.maxE[_nextIntervalIndex] < queryStart) {
                // Indexed intervals are all on the left from the query, break.
                currentIntervalIndex = -1;
                return;
            }

            Interval _nextInterval = parentAIList.intervals.get(_nextIntervalIndex);
            if(_nextInterval.from() <= queryEnd && _nextInterval.to() >= queryStart) {
                // Found a valid candidate.
                currentIntervalIndex = _nextIntervalIndex;
                return;
            }
        }
    }
}
