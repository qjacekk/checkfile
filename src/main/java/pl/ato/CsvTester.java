package pl.ato;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by jkaras on 14/12/2016.
 */
public class CsvTester extends FileTester {
    private int currentLine;
    private CSVReader reader;
    private int fieldsCount;
    private boolean isOpen;
    private String delimiter;
    private static String[] delimiters = {",",";","|",":","\t"};
    private static int GUESS_LINES = 3;


    public CsvTester(File inputFile, int noOfMostFrequentValues, boolean sorted, boolean mostFrequent, Properties filter, String delimiter) throws Exception {
        super(noOfMostFrequentValues, sorted, mostFrequent);
        this.inputFile = inputFile;
        this.filterProperties = filter;
        this.delimiter = delimiter;
        init();
        initFlags();
    }

    String guessDelimiter(File inputFile) throws InvalidParameterException {
        String[] lines = new String[GUESS_LINES]; // read 3 lines
        try {
            BufferedReader br = new BufferedReader(new FileReader(inputFile));
            for(int i=0;i<GUESS_LINES; lines[i] = br.readLine(), i++);
        } catch (IOException e) {
            throw new InvalidParameterException("Unable to read file : " + inputFile.getName() + " as CSV");
        }

        // check each predefined delimiter
        for(String delim : delimiters) {
            // assume the number of delimiters is exactly the same in all lines
            // 1. check number of occurences of the delimiter in the first line
            int delimCount = StringUtils.countMatches(lines[0], delim);
            if(delimCount > 0) {
                boolean found = true;
                // 2. check other lines
                for (int i = 1; i < GUESS_LINES; i++) {
                    if (StringUtils.countMatches(lines[i], delim) != delimCount) {
                        found = false;
                        break;
                    }
                }
                if(found) return delim;
            }
        }
        throw new InvalidParameterException("Unable to guess the delimiter. Try to provide it as a parameter");
    }

    @Override
    public void init() throws Exception {
        if(delimiter == null) { // delimiter has not been provided
            delimiter = guessDelimiter(inputFile);
        }
        CSVParser parser = new CSVParserBuilder().withSeparator(delimiter.charAt(0)).build();
        reader = new CSVReaderBuilder(new FileReader(inputFile)).withCSVParser(parser).build();
        String[] header = reader.readNext();
        fieldsCount = header.length;
        fields = new ArrayList<>(fieldsCount);
        types = new ArrayList<>(fieldsCount);
        for(String field : header) {
            fields.add(field);
            types.add("String");
        }
        // check filter and cast values to appropriate type (initially values are provided as strings from command line)
        if(filterProperties != null && filterProperties.size()>0) {
            filter = new HashMap<>(filterProperties.size());
            for(Map.Entry<Object,Object> me: filterProperties.entrySet()) {
                String key = (String) me.getKey();
                if(fields.contains(key)) {
                    filter.put(key, me.getValue());
                } else {
                    throw new InvalidParameterException("Filter error - field '" + key + "' not found in the file.");
                }
            }
        }
        currentLine = 0;
        isOpen = true;
    }

    @Override
    public String getFileInfo() {
        return "CSV delimiter: '" + delimiter + "' size: " + inputFile.length();
    }

    @Override
    public boolean hasNext() {
        return isOpen;
    }

    @Override
    public Map<String, Object> next() {
        try {
            String[] row = reader.readNext();
            if (row != null) {
                currentLine++;
                Map<String, Object> entry = new HashMap<>();
                if(row.length < fieldsCount) {
                    System.err.println("Invalid number of fields in line " + currentLine + " exp: "+fieldsCount + " got: "+row.length + " content: " + Arrays.toString(row));
                    return null;
                } else {
                    for (int i = 0; i < fieldsCount; i++) {
                        if (row[i].length() == 0) emptyString[i] = true;
                        else if (NULLSTR.equalsIgnoreCase(row[i])) nullString[i] = true;
                        else if (row[i].startsWith("-")) negative[i] = true;
                        entry.put(fields.get(i), row[i]);
                    }
                }
                return entry;
            } else {
                isOpen = false;
                return null;
            }
        } catch(IOException ioe){
            isOpen = false;
            return null;
        }
    }
}
