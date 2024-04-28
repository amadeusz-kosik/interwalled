package com.eternalsh.interwalled.algorithm;

import java.util.Iterator;

public class OverlapIterator<T> implements Iterator<Interval<T>>
{
    private final long queryStart;
    private final long queryEnd;

    private final AIList<T> parentAIList;
    private final long parentAIListComponentsCount;

    private int currentComponentIndex = 0;
    private int currentIntervalIndex = 0;

    // Flag to determine whether we should start
    // searching in a new component or stay in current one.
    private boolean mFindIvIdx = true;
    private boolean mBreakFromWhile = false;


    private Interval<T> nextInterval;

    public OverlapIterator(final long queryStart, final long queryEnd, final AIList<T> parentAIList)
    {
        this.queryStart = queryStart;
        this.queryEnd = queryEnd;
        this.parentAIList = parentAIList;
        this.parentAIListComponentsCount = parentAIList.getComponentsCount();

        nextInterval = getNextInterval();
    }

    @Override
    public boolean hasNext() {
        return nextInterval != null;
    }

    @Override
    public Interval<T> next() {
        Interval<T> currentReturn = nextInterval;
        nextInterval = getNextInterval();
        return currentReturn;
    }

    public Interval<T> getNextInterval() {
        // Iterate over all components
        while (currentComponentIndex < parentAIListComponentsCount) {
            final int currentComponentStartIndex = currentComponentStartIndex();
            final int currentComponentLength     = currentComponentLength();
            final int currentComponentEndIndex   = currentComponentStartIndex + currentComponentLength;

            // If component has more than 15 intervals then use binary search
            // to get right most interval. Otherwise, perform linear search.
            if (currentComponentLength > 15) {
                // Binary search approach. //
                if (mFindIvIdx) {
                    currentIntervalIndex = upperBound(currentComponentStartIndex, currentComponentEndIndex);

                    if (currentIntervalIndex < 0) {
                        pickNextComponent();
                        continue;
                    }

                    mFindIvIdx = false;
                }

                // Starting from found right most interval return all overlapping intervals until:
                //  we step outside of current component
                //  max end value is greater than start value of query interval
                //  mBreakFromWhile flag is set.
                while (currentIntervalIndex >= currentComponentStartIndex && currentComponentMaxEnd() >= queryStart && !mBreakFromWhile)
                {
                    final Interval<T> interval = parentAIList.getInterval(currentIntervalIndex);
                    currentIntervalIndex--;

                    if (currentIntervalIndex < 0) {
                        mBreakFromWhile = true;
                        currentIntervalIndex = 0;
                    }

                    if (interval.end() >= queryStart)
                        return interval;
                }
            } else {
                // Linear search approach. //
                if (currentIntervalIndex < currentComponentStartIndex)
                    currentIntervalIndex = currentComponentStartIndex;

                while (currentIntervalIndex < currentComponentEndIndex) {
                    final Interval<T> interval = currentInterval();
                    currentIntervalIndex ++;

                    if (interval.start() <= queryEnd && interval.end() >= queryStart)
                        return interval;
                }
            }

            mBreakFromWhile = false;
            mFindIvIdx = true;
            currentComponentIndex ++;
        }

        return null;
    }

    private int currentComponentStartIndex() {
        return parentAIList.getComponentStartIndex(currentComponentIndex);
    }

    private int currentComponentLength() {
        return parentAIList.getComponentLength(currentComponentIndex);
    }

    private long currentComponentMaxEnd() {
        return parentAIList.getComponentMaxEnd(currentComponentIndex);
    }

    private Interval<T> currentInterval() {
        return parentAIList.getInterval(currentIntervalIndex);
    }

    private void pickNextComponent() {
        currentComponentIndex ++;
        currentIntervalIndex = 0;

        mFindIvIdx = true;
    }

    protected int upperBound(final int currentComponentStart, final int currentComponentEnd) {
        int leftBound = currentComponentStart;
        int rightBound = currentComponentEnd;

        if (parentAIList.getInterval(rightBound - 1).start() <= queryEnd)
            return rightBound - 1;
        else if (parentAIList.getInterval(leftBound).start() > queryEnd)
            return -1;

        while (leftBound < rightBound) {
            int mid = leftBound + (rightBound - leftBound) / 2;

            if (parentAIList.getInterval(mid).start() <= queryEnd)
                leftBound = mid + 1;
            else
                rightBound = mid;
        }

        if (parentAIList.getInterval(leftBound).start() >= queryEnd)
            return leftBound - 1;
        else
            return leftBound;
    }
}
