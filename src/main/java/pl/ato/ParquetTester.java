package pl.ato;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.*;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by jkaras on 14/12/2016.
 */
public class ParquetTester extends FileTester {
    private final static Charset UTF8 = Charset.forName("UTF8");
    private ParquetReader<Group> reader;
    private MessageType gschema;
    private boolean isOpen;
    private ParquetMetadata metadata;

    public ParquetTester(File inputFile, int noOfMostFrequentValues, boolean sorted, boolean mostFrequent, Properties filter) throws Exception {
        super(inputFile, noOfMostFrequentValues, sorted, mostFrequent, filter);
    }

    @Override
    public void init() throws Exception {
        try {
            Path parquetFilePath = new Path(inputFile.toURI());
            Configuration conf = new Configuration();
            // set parallelism to 1
            conf.set(ParquetFileReader.PARQUET_READ_PARALLELISM, "1");
            GroupReadSupport readSupport = new GroupReadSupport();
            metadata = ParquetFileReader.readFooter(conf, parquetFilePath, ParquetMetadataConverter.NO_FILTER);
            gschema = metadata.getFileMetaData().getSchema();
            fields = new ArrayList<>(gschema.getFieldCount());
            types = new ArrayList<>(gschema.getFieldCount());
            javaTypes = new ArrayList<>(gschema.getFieldCount());
            for(int i=0; i<gschema.getFieldCount(); i++) {
                Type t = gschema.getType(i);
                fields.add(t.getName());
                PrimitiveType.PrimitiveTypeName tn = t.asPrimitiveType().getPrimitiveTypeName();
                if(t.getOriginalType() == OriginalType.UTF8) {
                    types.add("String (UTF8)");
                    javaTypes.add(String.class);
                } else if(tn.equals(PrimitiveType.PrimitiveTypeName.BINARY)) {
                    types.add("bytes (BINARY)");
                    javaTypes.add(tn.javaType);
                } else {
                    types.add(tn.javaType.getName() + " (" + tn.name() + ")");
                    javaTypes.add(tn.javaType);
                }
            }
            reader = ParquetReader.builder(readSupport, parquetFilePath).withConf(conf).build();
            isOpen = true;
            initFlags();

            // check filter and cast values to appropriate type (initially values are provided as strings from command line)
            if(filterProperties != null && filterProperties.size()>0) {
                filter = new HashMap<>(filterProperties.size());
                for(Map.Entry<Object,Object> me: filterProperties.entrySet()) {
                    String key = (String) me.getKey();
                    if(fields.contains(key)) {
                        Type t = gschema.getType(fields.indexOf(key));
                        PrimitiveType.PrimitiveTypeName tn = t.asPrimitiveType().getPrimitiveTypeName();
                        String value = (String) me.getValue();
                        try {
                            switch (tn) {
                                case BINARY:
                                    if (t.getOriginalType() == OriginalType.UTF8) {
                                        filter.put(key, value);
                                    } else throw new InvalidParameterException("Filtering on type "+tn.name() + " for field '" + key + "' is not supported");
                                    break;
                                case BOOLEAN:
                                    filter.put(key, Boolean.parseBoolean(value));
                                    break;
                                case DOUBLE:
                                    filter.put(key,Double.parseDouble(value));
                                    break;
                                case FLOAT:
                                    filter.put(key,Float.parseFloat(value));
                                    break;
                                case INT32:
                                    filter.put(key,Integer.parseInt(value));
                                    break;
                                case INT64:
                                    filter.put(key,Long.parseLong(value));
                                    break;
                                case INT96:
                                    filter.put(key,new BigInteger(value));
                                    break;
                                default:
                                    throw new InvalidParameterException("Filtering on type "+tn.name() + " for field '" + key + "' is not supported");
                            }
                        } catch( Exception e) {
                            throw new InvalidParameterException("Filter error - unable to cast value '"+value+"' to type "+tn.name() + " of field '" + key+"'");
                        }
                    } else {
                        throw new InvalidParameterException("Filter error - field '" + key + "' not found in the file.");
                    }
                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getFileInfo() {
        StringBuilder sb = new StringBuilder("PARQUET");
        long rowCount = 0;
        long compressedSize = 0;
        long totalByteSize = 0;

        CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
        for(BlockMetaData bmd : metadata.getBlocks()) {
            rowCount += bmd.getRowCount();
            compressedSize += bmd.getCompressedSize();
            totalByteSize += bmd.getTotalByteSize();
            for(ColumnChunkMetaData ccmd : bmd.getColumns()) {
                if(!codec.equals(ccmd.getCodec())) codec = ccmd.getCodec();
            }
        }
        sb.append(" compression: ").append(codec.toString());
        sb.append(" blocks: ").append(metadata.getBlocks().size());
        sb.append(" rowCount: ").append(rowCount);
        sb.append(" compressedSize: ").append(compressedSize);
        sb.append(" totalByteSize: ").append(totalByteSize);
        return sb.toString();
    }

    @Override
    public boolean hasNext() {
        return isOpen;
    }

    @Override
    public Map<String, Object> next() {
        try {
            Group g = reader.read();
            if(g!=null) {
                Map<String, Object> entry = new HashMap<>();
                GroupType schema = g.getType();
                for (int j = 0; j < schema.getFieldCount(); j++) {
                    if(g.getFieldRepetitionCount(j) > 0) {
                        Type t = schema.getType(j);
                        //OriginalType ot = t.getOriginalType();
                        Object value = null;
                        if (t.isPrimitive()) {
                            PrimitiveType.PrimitiveTypeName tn = t.asPrimitiveType().getPrimitiveTypeName();
                            switch (tn) {
                                case BINARY:
                                    if (t.getOriginalType() == OriginalType.UTF8) {
                                        byte[] content = g.getBinary(j, 0).getBytes();
                                        if(content.length == 0) emptyString[j] = true;
                                        String v = new String(content, UTF8);
                                        if(NULLSTR.equalsIgnoreCase(v)) nullString[j] = true;
                                        value = v;
                                    } else
                                        value = g.getBinary(j, 0);
                                    break;
                                case BOOLEAN:
                                    value = g.getBoolean(j, 0);
                                    break;
                                case DOUBLE:
                                    double d = g.getDouble(j, 0);
                                    if(d<0) negative[j] = true;
                                    value = d;
                                    break;
                                case FLOAT:
                                    float f = g.getFloat(j, 0);
                                    if(f<0) negative[j] = true;
                                    value = f;
                                    break;
                                case INT32:
                                    int i = g.getInteger(j, 0);
                                    if(i<0) negative[j] = true;
                                    value = i;
                                    break;
                                case INT64:
                                    long l = g.getLong(j, 0);
                                    if(l<0) negative[j] = true;
                                    value = l;
                                    break;
                                case INT96:
                                    BigInteger b = new BigInteger(g.getBinary(j, 0).getBytes());
                                    if(b.compareTo(BigInteger.ZERO)==-1) negative[j] = true;
                                    value = b;
                                    break;
                                case FIXED_LEN_BYTE_ARRAY:
                                    value = g.getBinary(j, 0).getBytes();
                                    break;
                            }
                        }
                        if (value != null) entry.put(schema.getFieldName(j), value);
                        else entry.put(schema.getFieldName(j), g.getValueToString(j, 0));
                    } else entry.put(schema.getFieldName(j), null);
                }
                return entry;
            } else {
                reader.close();
                isOpen = false;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
