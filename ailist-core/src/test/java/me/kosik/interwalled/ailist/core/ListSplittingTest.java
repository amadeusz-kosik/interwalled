package me.kosik.interwalled.ailist.core;

import me.kosik.interwalled.ailist.core.model.Configuration;
import me.kosik.interwalled.ailist.core.model.Interval;
import me.kosik.interwalled.ailist.core.benchmark.ListBuilder;
import me.kosik.interwalled.ailist.core.utils.ListUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class ListSplittingTest {

    final private List<Interval> basicData = Arrays.asList(
        new Interval( 1,  11),
        new Interval( 1,  12),
        new Interval( 2,  13),
        new Interval( 2,  14),
        new Interval( 3,  15),

        new Interval( 4,  64),

        new Interval( 4,  12),
        new Interval( 5,  10),
        new Interval( 6,  12),
        new Interval( 6,  14),
        new Interval( 7,  18),

        new Interval( 8,  64),
        new Interval( 8,  62),
        new Interval( 9,  63),
        new Interval( 9,  61),
        new Interval(10,  60),

        new Interval(10,  33),
        new Interval(11,  32),
        new Interval(12,  33),
        new Interval(12,  32),
        new Interval(15,  33)
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
                new Interval(0,  64),
                new Interval(0,  96),
                new Interval(0, 128),
                new Interval(1,   2),
                new Interval(2,   3),
                new Interval(3,   4),
                new Interval(4,   5),
                new Interval(5,   6),
                new Interval(6,   7),
                new Interval(7,   8),
                new Interval(8,   9),
                new Interval(9,  10)
            )
        );

        assertEquals(12, ListUtils.sum(aiLists));
        assertEquals( 2, aiLists.size(), "Outliers should be in a separate group.");
        assertEquals( 9, aiLists.get(0).intervals().length, "First component should contain most elements.");
        assertEquals( 3, aiLists.get(1).intervals().length, "Last component should store the outliers.");
    }
}
