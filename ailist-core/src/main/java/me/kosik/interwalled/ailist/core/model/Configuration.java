package me.kosik.interwalled.ailist.core.model;

public record Configuration(
    int intervalsCountToCheckLookahead,
    int intervalsCountToTriggerExtraction,
    int maximumComponentSize
) {
    final static public Configuration DEFAULT =
        new Configuration(24, 16, 100000);
}