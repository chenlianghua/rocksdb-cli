package org.geye.rocksdbCli;

import org.apache.commons.cli.*;
import org.geye.rocksdbCli.bean.DocNode;
import org.geye.rocksdbCli.bean.IndexNode;
import org.geye.rocksdbCli.query.Search;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class main {
    public static void main(String[] args) throws ParseException {
        CommandLineParser commandLineParser = new DefaultParser();
        Options opts = new Options();

        opts = opts.addOption("h", "help", false, "help info");
        opts = opts.addOption("e", "express", true, "search expression");

        opts = opts.addOption("a", "action", true, "action: search, count");

        opts = opts.addOption("gte", "startTime", false, "start datetime string, ie.\"2021-04-28 00:00:00\"");
        opts = opts.addOption("lte", "endTime", false, "end datetime string, ie.\"2021-04-28 00:00:00\"");

        opts = opts.addOption("l", "limit", false, "return size");

        CommandLine commandLine = commandLineParser.parse(opts, args);

        if (commandLine.hasOption("h")) {
            System.out.println("Help Message");
            System.exit(0);
        }

        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Calendar todayStart = Calendar.getInstance();
        todayStart.set(Calendar.HOUR_OF_DAY, 0);
        todayStart.set(Calendar.MINUTE, 0);
        todayStart.set(Calendar.SECOND, 0);
        todayStart.set(Calendar.MILLISECOND, 0);

        todayStart.add(Calendar.DATE, -30);

        long startTs = Timestamp.valueOf(commandLine.getOptionValue("gte", sdf.format(todayStart.getTime()))).getTime();
        long endTs = Timestamp.valueOf(commandLine.getOptionValue("lte", sdf.format(new Date()))).getTime();
        int limit = Integer.parseInt(commandLine.getOptionValue("limit", "10"));

        String action = "search";
        action = commandLine.getOptionValue("a", action);

        String expression = commandLine.getOptionValue("e");
        String indexType = expression.split("=")[0];
        String target = expression.split("=")[1];

        long t1 = System.currentTimeMillis();

        switch (action) {
            case "count":
                break;
            case "search":
            default:
                Search search = new Search(startTs, endTs, indexType, target, limit);
                List<DocNode> result = search.doQuery().result();
                for (DocNode node: result) {
                    System.out.println(node.toString());
                }
                break;
        }

        long t2 = System.currentTimeMillis();

        System.out.println(String.format("start ts: %s", t1));
        System.out.println(String.format("e n d ts: %s", t2));

        System.out.println(String.format("cost seconds: %s", (float) (t2 - t1) / 1000));
    }
}
