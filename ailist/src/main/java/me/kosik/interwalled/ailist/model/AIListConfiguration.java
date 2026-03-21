package me.kosik.interwalled.ailist.model;


/**
 *
 * @param maximumComponentsCount
 * @param intervalsCountToCheckLookahead
 * @param intervalsCountToTriggerExtraction
 * @param maximumComponentSize
 * @param isInputDataSorted
 */
public record AIListConfiguration(
        int maximumComponentsCount,
        int intervalsCountToCheckLookahead,
        int intervalsCountToTriggerExtraction,
        int maximumComponentSize,
        boolean checkLookbehindCoverage,
        boolean isInputDataSorted
) {

    final public static AIListConfiguration DEFAULT =
            new AIListConfiguration(32, 24, 16, 64, false, false);
}
