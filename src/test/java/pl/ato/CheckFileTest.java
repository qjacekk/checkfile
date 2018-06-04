package pl.ato;

import org.junit.Test;

import java.io.File;
import java.net.URL;

import static org.junit.Assert.*;

public class CheckFileTest extends TestBase {

    @Test
    public void getFileType() {
        File csv = getFileResource("simple.csv");
        CheckFile.FileType type = CheckFile.getFileType(csv, null);
        assert type.equals(CheckFile.FileType.CSV);

        File avro = getFileResource("avro_snappy");
        type = CheckFile.getFileType(avro, null);
        assert type.equals(CheckFile.FileType.AVRO);

        File parquet = getFileResource("parquet_snappy");
        type = CheckFile.getFileType(parquet, null);
        assert type.equals(CheckFile.FileType.PARQUET);
    }
}