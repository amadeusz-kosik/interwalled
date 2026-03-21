package me.kosik.interwalled.ailist;

import me.kosik.interwalled.ailist.model.AIListConfiguration;
import me.kosik.interwalled.ailist.model.Interval;
import me.kosik.interwalled.ailist.utils.ListBuilder;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ListSplittingTest {

    final private List<Interval<String>> basicData = Arrays.asList(
        new Interval<>("CH1",   1,  11, "NIL"),
        new Interval<>("CH1",   1,  12, "NIL"),
        new Interval<>("CH1",   2,  13, "NIL"),
        new Interval<>("CH1",   2,  14, "NIL"),
        new Interval<>("CH1",   3,  15, "NIL"),

        new Interval<>("CH1",   4,  64, "NIL"),

        new Interval<>("CH1",   4,  12, "NIL"),
        new Interval<>("CH1",   5,  10, "NIL"),
        new Interval<>("CH1",   6,  12, "NIL"),
        new Interval<>("CH1",   6,  14, "NIL"),
        new Interval<>("CH1",   7,  18, "NIL"),

        new Interval<>("CH1",   8,  64, "NIL"),
        new Interval<>("CH1",   8,  62, "NIL"),
        new Interval<>("CH1",   9,  63, "NIL"),
        new Interval<>("CH1",   9,  61, "NIL"),
        new Interval<>("CH1",  10,  60, "NIL"),

        new Interval<>("CH1",  10,  33, "NIL"),
        new Interval<>("CH1",  11,  32, "NIL"),
        new Interval<>("CH1",  12,  33, "NIL"),
        new Interval<>("CH1",  12,  32, "NIL"),
        new Interval<>("CH1",  15,  33, "NIL")
    );

    @Test
    void basicSplitting() {
        final AIListConfiguration aiListConfig =
            new AIListConfiguration(10, 5, 5, 3, true, false);

        final AIList<String> aiList =
            ListBuilder.buildAIList(aiListConfig, basicData);

        assertEquals(21, aiList.size());
        assertEquals(3, aiList.getComponentsCount(), "Intervals ending with > 60 should be placed in a separate component.");
    }

    @Test
    void forceMinimalComponentLength() {
        final AIListConfiguration aiListConfig =
            new AIListConfiguration(10, 5, 5, 25, true, false);

        final AIList<String> aiList =
            ListBuilder.buildAIList(aiListConfig, basicData);

        assertEquals(21, aiList.size());
        assertEquals(1, aiList.getComponentsCount(), "All intervals should be placed in the same component.");
    }

    @Test
    void forceMaximalComponentsCount() {
        final AIListConfiguration aiListConfig =
                new AIListConfiguration(1, 5, 5, 3, true, false);

        final AIList<String> aiList =
            ListBuilder.buildAIList(aiListConfig, basicData);

        assertEquals(21, aiList.size());
        assertEquals(1, aiList.getComponentsCount(), "All intervals should be placed in the same component.");
    }

    @Test
    void splitInitialOutlier() {
        final AIList<String> aiList = ListBuilder.buildAIList(
            new AIListConfiguration(4, 6, 4, 4, true, false),
            Arrays.asList(
                new Interval<>("CH1",   0,  64, "NIL"),
                new Interval<>("CH1",   0,  96, "NIL"),
                new Interval<>("CH1",   0, 128, "NIL"),
                new Interval<>("CH1",   1,   2, "NIL"),
                new Interval<>("CH1",   2,   3, "NIL"),
                new Interval<>("CH1",   3,   4, "NIL"),
                new Interval<>("CH1",   4,   5, "NIL"),
                new Interval<>("CH1",   5,   6, "NIL"),
                new Interval<>("CH1",   6,   7, "NIL"),
                new Interval<>("CH1",   7,   8, "NIL"),
                new Interval<>("CH1",   8,   9, "NIL"),
                new Interval<>("CH1",   9,  10, "NIL")
            )
        );

        assertEquals(12, aiList.size());
        assertEquals( 2, aiList.getComponentsCount(), "Outliers should be in a separate group.");
        assertEquals( 9, aiList.getComponentLength(0), "First component should contain most elements.");
        assertEquals( 3, aiList.getComponentLength(1), "Last component should store the outliers.");
    }
}
