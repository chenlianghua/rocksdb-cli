package org.geye.rocksdbCli.bean;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

public class QueryParams {

    private long startTs;
    private long endTs;
    private int limit;

    private HashMap<String, String[]> filters = new HashMap<>();

    public QueryParams() {
        long currentTs = new Date().getTime();

        Calendar todayStart = Calendar.getInstance();
        todayStart.set(Calendar.HOUR_OF_DAY, 0);
        todayStart.set(Calendar.MINUTE, 0);
        todayStart.set(Calendar.SECOND, 0);
        todayStart.set(Calendar.MILLISECOND, 0);

        long todayStartTs = todayStart.getTime().getTime();

        new QueryParams(todayStartTs, currentTs);
    }

    public QueryParams(long startTs, long endTs) {
        new QueryParams(startTs, endTs, 10);
    }

    public QueryParams(long startTs, long endTs, int limit) {
        this.startTs = startTs;
        this.endTs = endTs;
        this.limit = limit;
    }

    public long getStartTs() {
        return startTs;
    }

    public void setStartTs(long startTs) {
        this.startTs = startTs;
    }

    public long getEndTs() {
        return endTs;
    }

    public void setEndTs(long endTs) {
        this.endTs = endTs;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public HashMap<String, String[]> getFilters() {
        return filters;
    }

    public void setFilters(HashMap<String, String[]> filters) {
        this.filters = filters;
    }

    public void set(String field, String value) {
        if (value == null || value.equals("")) return;

        this.filters.put(field, value.split(","));
    }

    @Override
    public String toString() {
        return "QueryParams{" +
                "startTs=" + startTs +
                ", endTs=" + endTs +
                ", limit=" + limit +
                ", filters=" + filters.toString() +
                '}';
    }
}
