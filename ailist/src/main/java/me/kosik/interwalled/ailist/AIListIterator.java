package me.kosik.interwalled.ailist;

import me.kosik.interwalled.ailist.model.Interval;
import me.kosik.interwalled.ailist.utils.BinarySearch;
import java.util.Iterator;

public class AIListIterator<T> implements Iterator<Interval<T>>
{
    private final long queryStart;
    private final long queryEnd;

    private final AIList<T> parentAIList;
    private final long parentAIListComponentsCount;

    private int currentComponentIndex = -1;
    private int currentIntervalIndex = -1;

    private Interval<T> currentInterval;

    public AIListIterator(final long queryStart, final long queryEnd, final AIList<T> parentAIList)
    {
        this.queryStart = queryStart;
        this.queryEnd = queryEnd;
        this.parentAIList = parentAIList;
        this.parentAIListComponentsCount = parentAIList.getComponentsCount();

        checkNextComponent();
    }

    @Override
    public boolean hasNext() {
        return currentInterval != null;
    }

    @Override
    public Interval<T> next() {
        Interval<T> currentReturn = currentInterval;

        if(currentInterval != null)
            findNextInterval();

        return currentReturn;
    }

    private void checkNextComponent() {
        currentComponentIndex ++;
        currentIntervalIndex = 0;

        if(currentComponentIndex >= parentAIListComponentsCount) {
            currentInterval = null;
            currentIntervalIndex = -1;

            return;
        }

        final int  currentComponentStartIndex = parentAIList.getComponentStartIndex(currentComponentIndex);
        final int  currentComponentLength     = parentAIList.getComponentLength(currentComponentIndex);
        final int  currentComponentEndIndex   = currentComponentStartIndex + currentComponentLength - 1;
        final long currentComponentMaxEnd     = parentAIList.getComponentMaxEnd(currentComponentIndex);

        // Shortcut for completely disjoint intervals.
        //  1/ if components_max < query.start, skip
        if(currentComponentMaxEnd < queryStart) {
            checkNextComponent();
            return;
        }

        // Shortcut for completely disjoint intervals.
        //  2/ if first_component.min > query.end, skip
        if(this.parentAIList.getInterval(currentComponentStartIndex).from() > queryEnd) {
            checkNextComponent();
            return;
        }

        // Find the rightmost element satisfying interval.start < queryEnd condition:
        int rightmostIntervalIndex = BinarySearch.findRightmostBinary(
                parentAIList.getIntervals(),
                currentComponentStartIndex,
                currentComponentEndIndex,
                queryEnd
        );

        if(rightmostIntervalIndex == -1) {
            checkNextComponent();
        } else {
            // Point +1 since findNextInterval() will look one step to the left.
            currentIntervalIndex = rightmostIntervalIndex + 1;
            findNextInterval();
        }
    }

    public void findNextInterval() {
        if(currentComponentIndex >= parentAIListComponentsCount) {
            currentIntervalIndex = -1;
            currentInterval = null;

            return;
        }

        final int currentComponentStartIndex = parentAIList.getComponentStartIndex(currentComponentIndex);

        // Try picking next interval in the current component.
        //  In fact, try picking _previous_ interval, as the algorithm goes from right to left.
        //  Do not go beyond the current component
        int _nextIntervalIndex = currentIntervalIndex - 1;
        Interval<T> _nextInterval = null;

        // Iterate from right to left
        while(_nextIntervalIndex >= currentComponentStartIndex) {
            _nextInterval = parentAIList.getInterval(_nextIntervalIndex);

            if (_nextInterval.from() <= queryEnd && _nextInterval.to() >= queryStart) {
                break;
            } else if(parentAIList.getIntervalMaxEnd(_nextIntervalIndex) < queryStart) {
                _nextInterval = null;
                _nextIntervalIndex -= 1;

                break;
            } else {
                _nextInterval = null;
                _nextIntervalIndex -= 1;
            }
        }

        if(_nextInterval != null) {
            currentInterval         = _nextInterval;
            currentIntervalIndex    = _nextIntervalIndex;
        } else {
            checkNextComponent();
        }
    }
}
