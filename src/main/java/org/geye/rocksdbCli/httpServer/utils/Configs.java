package org.geye.rocksdbCli.httpServer.utils;

public class Configs {
    public static String DB_HOME = "/data0/rocksdb";
    public static String INDEX_HOME = String.format("%s/index", DB_HOME);
    public static String SESSIONS_HOME = String.format("%s/sessions", DB_HOME);
    public static String[] ALL_INDEX_TYPE = {"srcIp", "srcPort", "protocol", "dstIp", "dstPort"};
}
