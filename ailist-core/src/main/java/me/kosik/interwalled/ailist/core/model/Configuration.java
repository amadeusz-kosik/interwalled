package me.kosik.interwalled.ailist.core.model;

public record Configuration(
    int maximumComponentsCount,
    int intervalsCountToCheckLookahead,
    int intervalsCountToTriggerExtraction,
    int maximumComponentSize,
    boolean checkLookbehindCoverage
) {}
