package me.kosik.interwalled.ailist;


public class AIListConfig {
    final int maximumComponentsCount;
    final int intervalsCountToCheckLookahead;
    final int intervalsCountToTriggerExtraction;
    final int minimumComponentSize;

    public AIListConfig(
            final int maximumComponentsCount,
            final int intervalsCountToCheckLookahead,
            final int intervalsCountToTriggerExtraction,
            final int minimumComponentSize
    ) {
        this.maximumComponentsCount = maximumComponentsCount;
        this.intervalsCountToCheckLookahead = intervalsCountToCheckLookahead;
        this.intervalsCountToTriggerExtraction = intervalsCountToTriggerExtraction;
        this.minimumComponentSize = minimumComponentSize;
    }

    static public AIListConfig DEFAULT =
        new AIListConfig(10, 20, 10, 64);
}
