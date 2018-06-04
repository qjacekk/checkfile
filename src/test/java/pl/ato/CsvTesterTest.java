package pl.ato;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class CsvTesterTest extends TestBase {

    @Test
    public void testSimple() throws Exception {
        File testFile = getFileResource("simple.csv");
        FileTester tester = new CsvTester(testFile, 5, true, true, null, null);

        tester.test();

        // check if delimiter and size are correct
        assert tester.getFileInfo().equals("CSV delimiter: ',' size: 45814");
        // check total no. of rows
        assert tester.totalRows == 1000;
        // check 'count'
        int indexOfInt = tester.fields.indexOf("INTEGER");
        assert tester.coverage.get(indexOfInt).get() == 1000;

        // check if 'EMPTY_STRINGS' warning has been detected
        int indexOfVnull = tester.fields.indexOf("V_NULL");
        assert tester.emptyString[indexOfVnull];

        // check 'count' in frequency section
        int indexOfBoolean = tester.fields.indexOf("BOOLEAN");
        assert tester.valStats.get(indexOfBoolean).get("true").get() == 500;

    }


    @Test
    public void testQuotedWithNonStandardDelimiter() throws Exception {
        File testFile = getFileResource("quoted_sc_delim.csv");
        FileTester tester = new CsvTester(testFile, 5, true, true, null, null);

        tester.test();

        // check if delimiter and size are correct
        assert tester.getFileInfo().equals("CSV delimiter: ';' size: 65834");
        // check total no. of rows
        assert tester.totalRows == 1000;
        // check 'count'
        int indexOfInt = tester.fields.indexOf("INTEGER");
        assert tester.coverage.get(indexOfInt).get() == 1000;

        // check if 'EMPTY_STRINGS' warning has been detected
        int indexOfVnull = tester.fields.indexOf("V_NULL");
        assert tester.emptyString[indexOfVnull];

        // check 'count' in frequency section
        int indexOfBoolean = tester.fields.indexOf("BOOLEAN");
        assert tester.valStats.get(indexOfBoolean).get("true").get() == 500;

    }
}
