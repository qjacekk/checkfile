package pl.ato;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class ParquetTesterTest extends TestBase {

    @BeforeClass
    public static void muteLoggers() {
        // mute loggers first, parquet can be very noisy
        CheckFile.muteLoogers();
    }

    @Test
    public void testUncompressed() throws Exception {
        File testFile = getFileResource("parquet_uncompressed");
        FileTester tester = new ParquetTester(testFile, 5, true, true, null);

        tester.test();

        // check compression
        assert tester.getFileInfo().startsWith("PARQUET compression: UNCOMPRESSED");
        // check total no. of rows
        assert tester.totalRows == 1000;
        // check 'count'
        int indexOfInt = tester.fields.indexOf("integer");
        assert tester.coverage.get(indexOfInt).get() == 1000;

        // check 'count' for a field that has not been populated
        int indexOfVnull = tester.fields.indexOf("v_null");
        assert tester.coverage.get(indexOfVnull).get() == 0;
        // check if 'ALL_NULL' warning has been detected
        assert !tester.notNull[indexOfVnull];

        // check 'count' in frequency section
        int indexOfBoolean = tester.fields.indexOf("boolean");
        assert tester.valStats.get(indexOfBoolean).get(Boolean.TRUE).get() == 500;
    }


    @Test
    public void testSnappyCodec() throws Exception {
        File testFile = getFileResource("parquet_snappy");
        FileTester tester = new ParquetTester(testFile, 5, true, true, null);

        tester.test();

        // check compression
        assert tester.getFileInfo().startsWith("PARQUET compression: SNAPPY");
        // check total no. of rows
        assert tester.totalRows == 1000;
        // check 'count'
        int indexOfInt = tester.fields.indexOf("integer");
        assert tester.coverage.get(indexOfInt).get() == 1000;

        // check 'count' for a field that has not been populated
        int indexOfVnull = tester.fields.indexOf("v_null");
        assert tester.coverage.get(indexOfVnull).get() == 0;
        // check if 'ALL_NULL' warning has been detected
        assert !tester.notNull[indexOfVnull];

        // check 'count' in frequency section
        int indexOfBoolean = tester.fields.indexOf("boolean");
        assert tester.valStats.get(indexOfBoolean).get(Boolean.TRUE).get() == 500;
    }


    @Test
    public void toJSON() throws Exception {
        captureOutput();
        File testFile = getFileResource("parquet_snappy");
        FileTester tester = new ParquetTester(testFile, 5, true, true, null);

        tester.toJSON(1);
        restoreOutput();
        String result = out.toString().trim();
        // jackson does not guarantee that the order of the fields will be the same each time
        // (at least there seems to be a difference between Windows and Linux
        assert result.startsWith("{");
        assert result.contains("\"bytes\":\"31\"");
        assert result.contains("\"float\":0.001");
        assert result.endsWith("}");
    }

    @Test
    public void toCSV() throws Exception {
        captureOutput();
        File testFile = getFileResource("parquet_snappy");
        FileTester tester = new ParquetTester(testFile, 5, true, true, null);

        tester.toCSV(",", false, 1);
        restoreOutput();
        String output[] = out.toString().split("\\r?\\n");
        //for(String s: output) System.out.println(s);
        assert output.length == 2;
        assert "boolean,integer,long,float,double,string,bytes,v_zero,v_null,vs_null".equals(output[0]);
        assert "false,1,1,0.001,0.001,1,31,0,,Null".equals(output[1]);
    }
}