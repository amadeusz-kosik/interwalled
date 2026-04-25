package me.kosik.interwalled.ailist.core.benchmark;

import me.kosik.interwalled.ailist.core.model.Interval;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import static org.junit.jupiter.api.Assertions.assertEquals;


class SearchUtilsTest {


    @Test
    void returnsMinusOneWhenQueryIsBeforeAllIntervals() {
        var list = intervals(10, 30, 50);
        int index = SearchUtils.findRightmost(list, 5);

        assertEquals(-1, index);
    }

    @Test
    void returnsLastIndexWhenQueryIsAfterAllIntervals() {
        var list = intervals(10, 30, 50);
        int index = SearchUtils.findRightmost(list, 100);

        assertEquals(2, index);
    }

    @Test
    void returnsExactMatchingFromIndex() {
        var list = intervals(10, 30, 50);
        int index = SearchUtils.findRightmost(list, 30);

        assertEquals(1, index);
    }

    @Test
    void returnsRightmostIntervalBeforeQueryWhenQueryFallsBetweenIntervals() {
        var list = intervals(10, 30, 50);
        int index = SearchUtils.findRightmost(list, 45);

        assertEquals(1, index);
    }

    @Test
    void returnsFirstIndexWhenQueryEqualsFirstFrom() {
        var list = intervals(10, 30, 50);
        int index = SearchUtils.findRightmost(list, 10);

        assertEquals(0, index);
    }

    @Test
    void worksWithSingleElementList() {
        var list = intervals(10);

        assertEquals(0, SearchUtils.findRightmost(list, 10));
        assertEquals(-1, SearchUtils.findRightmost(list, 5));
        assertEquals(0, SearchUtils.findRightmost(list, 100));
    }

    @Test
    void handlesLargeListAndUsesNarrowingPath() {
        ArrayList<Interval> list = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            long from = i * 10L;
            list.add(new Interval(i, from, from + 5));
        }

        assertEquals(13, SearchUtils.findRightmost(list, 135));
        assertEquals(13, SearchUtils.findRightmost(list, 136));
    }


    private static ArrayList<Interval> intervals(long... fromValues) {
        ArrayList<Interval> result = new ArrayList<>();
        for (long from : fromValues) {
            result.add(new Interval(result.size(), from, from + 5));
        }
        return result;
    }
}
