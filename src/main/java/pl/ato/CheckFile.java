package pl.ato;

import ch.qos.logback.classic.Level;
import org.apache.commons.cli.*;
//import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.LogManager;

/**
 * Created by jkaras on 14/12/2016.
 */
public class CheckFile {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(CheckFile.class);

    private static final int MAX_MOST_FREQ = 5;
    private static final String DESCRIPTION = "Generate coverage and data validity report." ;
    private static void usage(Options options, String message) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("cf [OPTIONS] <file_name>", DESCRIPTION, options, message);
        System.exit(1);
    }
    public enum FileType {
        AVRO, PARQUET, CSV, JSON;
    }
    public static final String MAGIC_STR_PAR = "PAR1";
    public static final byte[] MAGIC_PAR = MAGIC_STR_PAR.getBytes(Charset.forName("ASCII"));
    public static final byte[] MAGIC_AVRO = new byte[]{(byte)79, (byte)98, (byte)106, (byte)1};

    public static FileType getFileType(File inputFile, String delimiter) {
        // check if AVRO or PARQUET
        try {
            if(delimiter != null || inputFile.getName().endsWith(".csv")) return FileType.CSV;
            if(inputFile.getName().endsWith(".json")) return FileType.JSON;

            DataInputStream dis = new DataInputStream(new FileInputStream(inputFile));
            byte[] magic = new byte[4];
            dis.read(magic);
            if(Arrays.equals(MAGIC_PAR, magic)) return FileType.PARQUET;
            else if(Arrays.equals(MAGIC_AVRO, magic)) return FileType.AVRO;
            dis.close();

            // TODO: try more sophisticated methods of guessing the file type here
            // e.g. check if text file, then check if CSV or JSON

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    public static void exit(String message, int returnCode) {
        System.out.println(message);
        System.exit(returnCode);
    }

    protected static void muteLoogers() {

        LogManager.getLogManager().reset(); // mutes codec
        //LogManager.getLogManager().getLogger("").setLevel(java.util.logging.Level.OFF);
        //((Logger) LoggerFactory.getLogger(KerberosName.class)).setLevel(Level.OFF);


        // mute parquet
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("root");
        root.setLevel(Level.OFF);

    }

    public static void main(String[] args) {
        muteLoogers();

        Options options = new Options();

        Option filterOpts = Option.builder("f").argName("filed=value").numberOfArgs(2).desc("generate report only for rows matching the filter").build();
        options.addOption(filterOpts);
        options.addOption("ns", false, "do not sort fields alphabetically in the report (use the original file order)");
        options.addOption("lf", false, "print least frequent samples (default: most frequent");
        options.addOption("m", true, "number of sample values to include in the report (default 5)");
        options.addOption("d", true, "CSV delimiter (if not specified ftest will try to guess)");
        options.addOption("h", false, "help");
        options.addOption("j", false, "convert to JSON (instead of generating coverage report");
        options.addOption("c", false, "convert to CSV (instead of generating coverage report");
        options.addOption("q", false, "enable quoting strings (only if -c was specified, this may slow things down)");
        options.addOption("n", true, "number of rows in CSV or JSON output (all by default");
        int mf = MAX_MOST_FREQ;
        CommandLineParser parser = new DefaultParser();
        String fileName = null;
        try {
            CommandLine cli = parser.parse(options, args);
            if(cli.hasOption("h")) {
                usage(options, "Examples:\ncf data_file.parquet > coverage_report.txt\ncf -f customer_id=10 -m 10 data_file.avro > coverage_report.txt");
            }

            if(cli.hasOption("")) {
                usage(options, null);
            }
            if(cli.getArgs().length < 1) {
                usage(options, "<file_name> must be specified\n");
            }
            fileName = cli.getArgs()[0];
            try {
                if (cli.hasOption("m")) mf = Integer.parseInt(cli.getOptionValue("m"));
            } catch(NumberFormatException nfe) {
                exit("m option must be an integer",1);
            }
            boolean sort = cli.hasOption("ns") ? false : true;
            boolean mostFrequent = cli.hasOption("lf") ? false : true;
            String delimiter = cli.hasOption("d") ? cli.getOptionValue("d") : null;
            if(delimiter != null && delimiter.length() > 1) throw new IllegalArgumentException("Delimiter: '" + delimiter + "' is not valid, it must be a single character");
            Properties filter = cli.hasOption("f") ? cli.getOptionProperties("f") : null;
            boolean toCsv = cli.hasOption("c");
            boolean toJson = cli.hasOption("j");
            if(toCsv && toJson) throw new IllegalArgumentException("Use either -c or -j, not both.");
            boolean quote = cli.hasOption("q");
            int maxNoOfRecords = -1;
            try {
                if (cli.hasOption("n")) {
                    maxNoOfRecords = Integer.parseInt(cli.getOptionValue("n"));
                    if(maxNoOfRecords <0) exit("n must be positive",1);
                }
            } catch(NumberFormatException nfe) {
                exit("n option must be an integer",1);
            }

            File inputFile = new File(fileName);
            if(inputFile.canRead()) {
                FileType type = getFileType(inputFile, delimiter);
                FileTester tester = null;
                if(type != null) {
                    switch(type) {
                        case PARQUET: tester = new ParquetTester(inputFile, mf, sort, mostFrequent, filter);
                            break;
                        case AVRO: tester = new AvroTester(inputFile, mf, sort, mostFrequent, filter);
                            break;
                        case CSV: tester = new CsvTester(inputFile, mf, sort, mostFrequent, filter, delimiter);
                            break;
                        default:
                            exit("Unsupported file format",1);
                    }
                    if(toCsv) {
                        if(type != FileType.CSV) tester.toCSV(delimiter, quote, maxNoOfRecords);
                        else exit("The file is already a CSV file",1);
                    } else if(toJson) {
                        tester.toJSON(maxNoOfRecords);
                    } else {
                        tester.test();
                    }
                } else {
                    exit("Unsupported file format",1);
                }
            } else {
                exit("Unable to read file: " + fileName, 1);
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
