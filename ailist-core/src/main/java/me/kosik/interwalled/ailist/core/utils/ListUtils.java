package me.kosik.interwalled.ailist.core.utils;

import me.kosik.interwalled.ailist.core.AIList;

import java.util.List;

public class ListUtils {

    public static long sum(final List<AIList> aiLists) {
        long sum = 0;

        for(AIList aiList : aiLists)
            sum += aiList.size();

        return sum;
    }
}
