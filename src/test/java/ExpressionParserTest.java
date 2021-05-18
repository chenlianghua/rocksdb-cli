import org.apache.commons.cli.*;
import org.geye.rocksdbCli.ExpressionParser;

public class ExpressionParserTest {
    public static void main(String[] args) throws ParseException {
        CommandLineParser commandLineParser = new DefaultParser();
        Options opts = new Options();

        opts = opts.addOption("h", "help", false, "help info");
        opts = opts.addOption("e", "express", true, "search expression");
        CommandLine commandLine = commandLineParser.parse(opts, args);

        String expression = commandLine.getOptionValue("e");

        ExpressionParser parser = new ExpressionParser();

        parser.parse(expression);
    }
}
