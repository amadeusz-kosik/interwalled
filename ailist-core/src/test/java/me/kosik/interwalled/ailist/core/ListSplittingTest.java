package me.kosik.interwalled.ailist.core;

import me.kosik.interwalled.ailist.core.model.Configuration;
import me.kosik.interwalled.ailist.core.model.Interval;
import me.kosik.interwalled.ailist.core.benchmark.ListBuilder;
import me.kosik.interwalled.ailist.core.benchmark.ListUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class ListSplittingTest {

    final private List<Interval> basicData = Arrays.asList(
        new Interval(  1,   1,  11),
        new Interval(  2,   1,  12),
        new Interval(  3,   2,  13),
        new Interval(  4,   2,  14),
        new Interval(  5,   3,  15),

        new Interval(  6,   4,  64),

        new Interval(  7,   4,  12),
        new Interval(  8,   5,  10),
        new Interval(  9,   6,  12),
        new Interval( 10,   6,  14),
        new Interval( 11,   7,  18),

        new Interval( 12,   8,  64),
        new Interval( 13,   8,  62),
        new Interval( 14,   9,  63),
        new Interval( 15,   9,  61),
        new Interval( 16,  10,  60),

        new Interval( 17,  10,  33),
        new Interval( 18,  11,  32),
        new Interval( 19,  12,  33),
        new Interval( 20,  12,  32),
        new Interval( 21,  15,  33)
    );

    @Test
    void basicSplitting() {
        final Configuration aiListConfig =
            new Configuration(6, 5, 64);

        final List<AIList> aiLists =
            ListBuilder.buildAILists(aiListConfig, basicData);

        System.out.println(aiLists);

        assertEquals(21, ListUtils.sum(aiLists));
        assertEquals(2, aiLists.size());
    }

    @Test
    void forceMaximalComponentsCount() {
        final Configuration aiListConfig =
                new Configuration(5, 5, 64);

        final List<AIList> aiLists =
            ListBuilder.buildAILists(aiListConfig, basicData);

        assertEquals(21, ListUtils.sum(aiLists));
        assertEquals(1, aiLists.size(), "All intervals should be placed in the same component.");
    }

    @Test
    void splitInitialOutlier() {
        final List<AIList> aiLists = ListBuilder.buildAILists(
            new Configuration(6, 3, 64),
            Arrays.asList(
                new Interval(  1,   0,  64),
                new Interval(  2,   0,  96),
                new Interval(  3,   0, 128),
                new Interval(  4,   1,   2),
                new Interval(  5,   2,   3),
                new Interval(  6,   3,   4),
                new Interval(  7,   4,   5),
                new Interval(  8,   5,   6),
                new Interval(  9,   6,   7),
                new Interval( 10,   7,   8),
                new Interval( 11,   8,   9),
                new Interval( 12,   9,  10)
            )
        );

        assertEquals(12, ListUtils.sum(aiLists));
        assertEquals( 2, aiLists.size(), "Outliers should be in a separate group.");
        assertEquals( 9, aiLists.get(0).size(), "First component should contain most elements.");
        assertEquals( 3, aiLists.get(1).size(), "Last component should store the outliers.");
    }
}
