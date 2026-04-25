package me.kosik.interwalled.ailist.core;


import me.kosik.interwalled.ailist.core.model.Interval;

import java.util.ArrayList;
import java.util.Map;

public class DataSources {

    public static void initializeDatabaseSources(final int databaseRowsCount, final Map<String, ArrayList<Interval>> databaseSources) {
        databaseSources.put("consecutiveIntervals",     DataGenerator.consecutive(databaseRowsCount));
        databaseSources.put("overlappingIntervals",     DataGenerator.overlapping(databaseRowsCount));
        databaseSources.put("lastingIntervals",         DataGenerator.lasting(databaseRowsCount));
        databaseSources.put("shortPoissonIntervals",    DataGenerator.shortPoisson(databaseRowsCount));
        databaseSources.put("mixed1Intervals",          DataGenerator.mixed(databaseRowsCount, 1));
        databaseSources.put("mixed2Intervals",          DataGenerator.mixed(databaseRowsCount / 2, 2));
        databaseSources.put("mixed3Intervals",          DataGenerator.mixed(databaseRowsCount / 3, 3));
        databaseSources.put("mixed4Intervals",          DataGenerator.mixed(databaseRowsCount / 4, 4));
    }

    public static void initializeDatabaseSources(final String databaseRowsCount, final Map<String, ArrayList<Interval>> databaseSources) {
        initializeDatabaseSources(parseInt(databaseRowsCount), databaseSources);
    }

    public static void initializeQuerySources(final int databaseRowsCount, final int queryRowsCount, final Map<String, ArrayList<Interval>> querySources) {
        querySources.put("querySparse", DataGenerator.querySparse(databaseRowsCount, queryRowsCount));
        querySources.put("queryDense",  DataGenerator.queryDense(databaseRowsCount, queryRowsCount));
    }

    public static void initializeQuerySources(final String databaseRowsCount, final String queryRowsCount, final Map<String, ArrayList<Interval>> querySources) {
        initializeQuerySources(parseInt(databaseRowsCount), parseInt(queryRowsCount), querySources);
    }

    private static int parseInt(final String asString) {
        return Integer.parseInt(asString.replace("_", ""));
    }
}
