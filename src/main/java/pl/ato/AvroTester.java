package pl.ato;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;

import java.io.File;
import java.util.*;

/**
 * Created by jkaras on 14/12/2016.
 */
public class AvroTester extends FileTester {
    private GenericRecord user;
    private DataFileReader<GenericRecord> dataFileReader;
    private Schema schema;
    private int fieldsCount;
    List<Schema.Type> effectiveTypes;

    private static Map<Schema.Type, Class> schemaTypeToClass = new HashMap<>();
    static {
        //schemaTypeToClass.put(Schema.Type.RECORD, Object.class);
        schemaTypeToClass.put(Schema.Type.ENUM, Enum.class);
        //schemaTypeToClass.put(Schema.Type.ARRAY, Object.class);
        //schemaTypeToClass.put(Schema.Type.MAP, Map.class);
        //schemaTypeToClass.put(Schema.Type.UNION, Object.class);
        //schemaTypeToClass.put(Schema.Type.FIXED, Object.class);
        schemaTypeToClass.put(Schema.Type.STRING, String.class);
        schemaTypeToClass.put(Schema.Type.BYTES, Object.class);
        schemaTypeToClass.put(Schema.Type.INT, Integer.class);
        schemaTypeToClass.put(Schema.Type.LONG, Long.class);
        schemaTypeToClass.put(Schema.Type.FLOAT, Float.class);
        schemaTypeToClass.put(Schema.Type.DOUBLE, Double.class);
        schemaTypeToClass.put(Schema.Type.BOOLEAN, Boolean.class);
        schemaTypeToClass.put(Schema.Type.NULL, Object.class);
    }


    public AvroTester(File inputFile, int noOfMostFrequentValues, boolean sorted, boolean mostFrequent, Properties filter) throws Exception {
        super(inputFile, noOfMostFrequentValues, sorted, mostFrequent, filter);
    }

    @Override
    public void init() throws Exception {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        dataFileReader = new DataFileReader<>(inputFile, datumReader);
        schema = dataFileReader.getSchema();
        fieldsCount = schema.getFields().size();
        fields = new ArrayList<>(fieldsCount);
        types = new ArrayList<>(fieldsCount);
        javaTypes = new ArrayList<>(fieldsCount);
        effectiveTypes = new ArrayList<>(fieldsCount);
        for(int i=0; i<schema.getFields().size(); i++) {
            Schema.Field field = schema.getFields().get(i);
            fields.add(field.name());
            Schema.Type t = field.schema().getType();
            if(t == Schema.Type.UNION) {
                for(Schema s : field.schema().getTypes()) {
                    if(s.getType() != Schema.Type.UNION && s.getType() != Schema.Type.NULL) t = s.getType();
                }
            }
            effectiveTypes.add(t);
            javaTypes.add(schemaTypeToClass.get(t));
            types.add(t.getName());
        }
        // check filter and cast values to appropriate type (initially values are provided as strings from command line)
        if(filterProperties != null && filterProperties.size()>0) {
            filter = new HashMap<>(filterProperties.size());
            for(Map.Entry<Object,Object> me: filterProperties.entrySet()) {
                String key = (String) me.getKey();
                if(fields.contains(key)) {
                    Schema.Type type = effectiveTypes.get(fields.indexOf(key));
                    String value = (String) me.getValue();
                    try {
                        switch (type) {
                            case BOOLEAN:
                                filter.put(key, Boolean.parseBoolean(value));
                                break;
                            case STRING:
                                filter.put(key, new Utf8(value));
                                break;
                            case INT:
                                filter.put(key,Integer.parseInt(value));
                                break;
                            case LONG:
                                filter.put(key,Long.parseLong(value));
                                break;
                            case FLOAT:
                                filter.put(key,Float.parseFloat(value));
                                break;
                            case DOUBLE:
                                filter.put(key,Double.parseDouble(value));
                                break;
                            default:
                                throw new InvalidParameterException("Filtering on type "+type.getName() + " for field '" + key + "' is not supported");
                        }
                    } catch( Exception e) {
                        throw new InvalidParameterException("Filter error - unable to cast value'"+value+"' to type "+type.getName() + " of field '" + key+"'");
                    }
                } else {
                    throw new InvalidParameterException("Filter error - field '" + key + "' not found in the file.");
                }
            }
        }
    }

    @Override
    public String getFileInfo() {
        StringBuilder sb = new StringBuilder("AVRO ");
        sb.append("compression: ").append(dataFileReader.getMetaString("avro.codec"));
        return sb.toString();
    }

    @Override
    public boolean hasNext() {
        return dataFileReader.hasNext();
    }

    @Override
    public Map<String, Object> next() {

        user = dataFileReader.next();

        if (user == null) return null;

        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fieldsCount; i++) {
            String name = fields.get(i);
            Object value = user.get(i);

            if (value != null) {
                Schema.Type et = effectiveTypes.get(i);
                switch (et) {
                    case STRING:
                        Utf8 v = (Utf8) value;
                        if (v.length() == 0) emptyString[i] = true;
                        else if (NULLSTR.equalsIgnoreCase(v.toString()))
                            nullString[i] = true;
                        // convert Utf8 to String
                        value = v.toString();
                        break;
                    case INT:
                        if (((Integer) value) < 0) negative[i] = true;
                        break;
                    case LONG:
                        if (((Long) value) < 0) negative[i] = true;
                        break;
                    case FLOAT:
                        if (((Float) value) < 0) negative[i] = true;
                        break;
                    case DOUBLE:
                        if (((Double) value) < 0) negative[i] = true;
                        break;
                }
            }
            entry.put(name, value);
        }
        return entry;
    }
}
