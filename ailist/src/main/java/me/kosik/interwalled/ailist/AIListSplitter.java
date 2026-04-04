package me.kosik.interwalled.ailist;

import me.kosik.interwalled.ailist.model.Intervals;

import java.util.Arrays;

public class AIListSplitter {

    public static AIList[] split(final AIList ailist) {
        AIList[] result = new AIList[ailist.componentsCount()];

        for(int componentIndex = 0; componentIndex < ailist.componentsCount(); componentIndex++) {
            int componentStart = ailist.componentsStartIndexes()[componentIndex];
            int componentEnd   = ailist.componentsStartIndexes()[componentIndex] + ailist.componentsLengths()[componentIndex];
            result[componentIndex] = new AIList(
                    new Intervals(Arrays.copyOfRange(ailist.intervals().intervals, componentStart, componentEnd)),
                    1,
                    new int[] { ailist.componentsLengths()[componentIndex] },
                    new int[] { 0 },
                    Arrays.copyOfRange(ailist.componentsMaxEnds(), componentStart, componentEnd)
            );
        }

        return result;
    }
}
