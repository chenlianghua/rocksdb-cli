package org.geye.rocksdbCli.bean;


public class QueryParams {

    private String target;
    private long startTs;
    private long endTs;
    private int limit;

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
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

    @Override
    public String toString() {
        return "QueryParams{" +
                "target='" + target + '\'' +
                ", startTs=" + startTs +
                ", endTs=" + endTs +
                ", limit=" + limit +
                '}';
    }
}
