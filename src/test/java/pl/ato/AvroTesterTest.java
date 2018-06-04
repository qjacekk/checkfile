package pl.ato;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class AvroTesterTest extends TestBase {

    @Test
    public void testNullCodec() throws Exception {
        File testFile = getFileResource("avro_null_codec");
        FileTester tester = new AvroTester(testFile, 5, true, true, null);

        tester.test();

        // check compression
        assert tester.getFileInfo().equals("AVRO compression: null");
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
        File testFile = getFileResource("avro_snappy");
        FileTester tester = new AvroTester(testFile, 5, true, true, null);

        tester.test();

        // check compression
        assert tester.getFileInfo().equals("AVRO compression: snappy");
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

    // TODO: captureOutput() and restoreOutput() should be defined in @Before
    // and @After so consider moving all toJSON and toCSV tests to separate class
    @Test
    public void toJSON() throws Exception {
        captureOutput();
        File testFile = getFileResource("avro_snappy");
        FileTester tester = new AvroTester(testFile, 5, true, true, null);

        tester.toJSON(1);
        restoreOutput();
        String result = out.toString().trim();
        assert result.startsWith("{");
        assert result.contains("\"bytes\":\"313233343536373930\"");
        assert result.contains("\"integer\":123456790");
        assert result.contains("\"string\":\"123456790\"");
        assert result.contains("\"v_null\":null");
        assert result.contains("\"v_zero\":0");
        assert result.contains("\"boolean\":true");
        assert result.contains("\"double\":123456.79");
        assert result.contains("\"long\":123456790");
        assert result.contains("\"float\":123456.79");
        assert result.contains("\"vs_null\":\"Null\"");
        assert result.endsWith("}");
    }

    @Test
    public void toCSV() throws Exception {
        captureOutput();
        File testFile = getFileResource("avro_null_codec");
        FileTester tester = new AvroTester(testFile, 5, true, true, null);

        tester.toCSV(",", false, 1);
        restoreOutput();
        String output[] = out.toString().split("\\r?\\n");
        //for(String s: output) System.out.println(s);
        assert output.length == 2;
        assert "boolean,integer,long,float,double,string,bytes,v_zero,v_null,vs_null".equals(output[0]);
        assert "false,1,1,0.001,0.001,1,31,0,,Null".equals(output[1]);
    }


}