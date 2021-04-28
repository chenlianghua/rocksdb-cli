import org.apache.commons.cli.*;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class main {
    public static void main(String[] args) throws ParseException {
        CommandLineParser commandLineParser = new DefaultParser();
        Options opts = new Options();

        opts.addOption("h", "help", false, "help info");
        opts.addOption("e", "express", true, "search expression");

        opts.addOption("a", "action", false, "action: search, count");

        opts.addOption("gte", "startTime", false, "start datetime string, ie.\"2021-04-28 00:00:00\"");
        opts.addOption("lte", "endTime", false, "end datetime string, ie.\"2021-04-28 00:00:00\"");

        opts.addOption("l", "limit", false, "return size");

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

        long startTs = Timestamp.valueOf(commandLine.getOptionValue("gte", sdf.format(todayStart.getTime()))).getTime();
        long endTs = Timestamp.valueOf(commandLine.getOptionValue("lte", sdf.format(new Date()))).getTime();
        int limit = Integer.parseInt(commandLine.getOptionValue("limit", "10"));

        RocksdbReader rocksdbReader = new RocksdbReader();
        rocksdbReader.doSearch(startTs, endTs, "srcIp", "192.168.10.1", limit);
    }
}
