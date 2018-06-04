package pl.ato;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.io.api.Binary;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jkaras on 14/12/2016.
 */
public abstract class FileTester implements Iterable<Map<String,Object>>, Iterator<Map<String,Object>> {

    protected final static String NULLSTR = "null";
    protected final static int MAX_BB_BYTES = 8;
    private int noOfMostFrequentValues;
    private boolean sorted;
    private boolean mostFrequent;
    protected Properties filterProperties;
    protected File inputFile;
    protected List<String> fields;
    protected List<String> types;
    protected List<Class> javaTypes;
    protected List<AtomicInteger> coverage;
    protected List<Map<Object,AtomicInteger>> valStats;
    protected boolean[] notNull;
    protected boolean[] emptyString;
    protected boolean[] negative;
    protected boolean[] nullString;
    protected Map<String,Object> filter;

    public double totalRows;


    public FileTester(int noOfMostFrequentValues, boolean sorted, boolean mostFrequent) {
        this.noOfMostFrequentValues = noOfMostFrequentValues;
        this.sorted = sorted;
        this.mostFrequent = mostFrequent;
    }

    public FileTester(File inputFile, int noOfMostFrequentValues, boolean sorted, boolean mostFrequent, Properties filter) throws Exception {
        this(noOfMostFrequentValues, sorted, mostFrequent);
        this.inputFile = inputFile;
        this.filterProperties = filter;
        init();
        initFlags();
    }

    public Iterator<Map<String, Object>> iterator() {
        return this;
    }

    /**
     * Initialize open file, initialize reader.
     * Initialize fields and types.
     * @throws Exception
     */
    public abstract void init() throws Exception;
    public abstract String getFileInfo();

    protected void initFlags() {
        int size = fields.size();
        notNull = new boolean[size];
        emptyString = new boolean[size];
        negative = new boolean[size];
        nullString = new boolean[size];
    }

    // Just for Iterator interface
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }


    private Comparator<Map.Entry<Object,AtomicInteger>> comparatorAsc = new Comparator<Map.Entry<Object, AtomicInteger>>() {
        @Override
        public int compare(Map.Entry<Object, AtomicInteger> o1, Map.Entry<Object, AtomicInteger> o2) {
            int vx = o1.getValue().get();
            int vy = o2.getValue().get();
            if(vx==vy) {
                Object ky = o1.getKey();
                if(ky instanceof Comparable) return ((Comparable) ky).compareTo(o2.getKey());
                else return 0;
            }
            return (vx < vy) ? -1 : 1;
        }
    };
    private Comparator<Map.Entry<Object,AtomicInteger>> comparatorDesc = new Comparator<Map.Entry<Object, AtomicInteger>>() {
        @Override
        public int compare(Map.Entry<Object, AtomicInteger> o1, Map.Entry<Object, AtomicInteger> o2) {
            int vx = o2.getValue().get();
            int vy = o1.getValue().get();
            if(vx==vy) {
                Object ky = o1.getKey();
                if(ky instanceof Comparable) return ((Comparable) ky).compareTo(o2.getKey());
                else return 0;
            }
            return (vx < vy) ? -1 : 1;
        }
    };

    protected List<Map.Entry<Object,AtomicInteger>> sortedList(Map<Object, AtomicInteger> map) {
        LinkedList<Map.Entry<Object,AtomicInteger>> lst = new LinkedList<>(map.entrySet());
        if(mostFrequent) Collections.sort(lst, comparatorDesc);
        else Collections.sort(lst, comparatorAsc);
        return lst;
    }
    private void printFilter() {
        for(String k:filter.keySet())
            if(filter.get(k) instanceof String) System.out.println("   " + k + " = '" + filter.get(k) +"'");
            else System.out.println("   " + k + " = " + filter.get(k));
    }

    public void test() {
        int noOfFields = fields.size();
        coverage = new ArrayList<>(noOfFields);
        valStats = new ArrayList<>(noOfFields);

        for(int i=0; i<noOfFields; i++) {
            coverage.add(new AtomicInteger(0));
            valStats.add(new HashMap<Object,AtomicInteger>());
        }

        totalRows = 0;
        long start = System.currentTimeMillis();
        for(Map<String, Object> row : this) {
            if (row != null) {
                if (filter != null) {
                    boolean match = true;
                    for(String k : filter.keySet()) {
                        if(!row.containsKey(k) || !filter.get(k).equals(row.get(k))) {
                            match = false;
                            break;
                        }
                    }
                    if(!match) continue; // skip this record
                }

                totalRows++;
                for(int i=0; i<noOfFields; i++) {
                    String field = fields.get(i);
                    Object value = row.get(field);
                    if (value != null) {
                        coverage.get(i).incrementAndGet();
                        Map<Object, AtomicInteger> stat = valStats.get(i);
                        if (stat.containsKey(value)) {
                            stat.get(value).incrementAndGet();
                        } else {
                            stat.put(value, new AtomicInteger(1));
                        }
                        notNull[i] = true;
                    }
                }
            }
        }
        long stop = System.currentTimeMillis();
        System.out.println("Tested in " + (stop - start) + " ms");

        List<String> fieldsSorted = fields;
        if(sorted) {
            List<String> nlist = new ArrayList<>(fields);
            Collections.sort(nlist);
            fieldsSorted = nlist;
        }


        // File info
        System.out.println("File: " + inputFile.getName());
        System.out.println("Info: " + getFileInfo());


        if(totalRows < 1) {
            if(filter != null) {
                System.out.println("Zero rows matching the filter:");
                printFilter();
            } else System.out.println("No data found.");
            return;
        }


        // coverage report
        System.out.println("=================");
        System.out.println(" Coverage report ");
        System.out.println("=================");

        if(filter != null) {
            System.out.println("Stats for filter:");
            printFilter();
            System.out.println("Total number of rows matching the filter:       " + (int) totalRows);
        } else {
            System.out.println("Total number of rows:            " + (int) totalRows);
        }



        String headerTemplate =    "%-30s : %-8s : %-8s : %-16s : %s";
        String template = "%-30s : %-8d : %-8.2f : %-16s : %s";
        String h1 = String.format(headerTemplate, "field", "count", "coverage", "type", "comment");
        System.out.println(h1);
        System.out.println(StringUtils.repeat("-", h1.length()));
        for(String field: fieldsSorted) {
            int i = fields.indexOf(field);
            int cnt = coverage.get(i).get();
            String comment = "";
            if(!notNull[i]) comment += "ALL_NULL ";
            if(negative[i]) comment += "NEGATIVE ";
            if(emptyString[i]) comment += "EMPTY_STRINGS ";
            if(nullString[i]) comment += "NULL_LITERALS ";
            System.out.println(String.format(template, field, cnt, 100 * cnt / totalRows, types.get(i), comment));
        }

        // n  most/least frequent values

        String title2 = mostFrequent ? String.format("%d most frequent values", noOfMostFrequentValues) : String.format("%d least frequent values", noOfMostFrequentValues);
        System.out.println();
        System.out.println(title2);
        System.out.println(StringUtils.repeat("=", title2.length()));
        String header2Template =    "%-30s : %-8s : %-8s : %-16s";
        String h2 = String.format(header2Template, "field", "count", "frequency", "value");
        System.out.println(h2);
        System.out.println(StringUtils.repeat("-", h2.length()));
        String template2 = "%-30s : %-8d : %-8.2f : ";

        for(String field: fieldsSorted) {
            int i = fields.indexOf(field);
            System.out.println(field);
            if (valStats.get(i).size() == 0) {
                System.out.println(String.format("%-30s : %s","","--- NOT AVAILABLE ---"));
            } else {
                List<Map.Entry<Object, AtomicInteger>> valuesList = sortedList(valStats.get(i));
                for (int j = 0; j < noOfMostFrequentValues; j++) {
                    if (j >= valuesList.size()) break;
                    Map.Entry<Object, AtomicInteger> me = valuesList.get(j);
                    int count = me.getValue().get();
                    System.out.print(String.format(template2, "", count, 100* count / totalRows));
                    Object value = me.getKey();
                    if(value instanceof Binary) {
                        // org.apache.parquet.io.api.Binary is used by Parquet
                        // .toString() is like Binary{x constant bytes, [12, 34, 56, 78, ...]}
                        // so convert to ByteBuffer first
                        value = ((Binary) value).toByteBuffer();
                    }
                    if(value instanceof ByteBuffer) {
                        // BB instance to string would be like java.nio.HeapByteBuffer[pos=0 lim=x cap=y]'
                        // so convert to hex string instead (but not more than 16 digits)
                        ByteBuffer bb = (ByteBuffer) value;
                        boolean threedots = bb.remaining() > MAX_BB_BYTES;
                        int len = Math.min(bb.remaining(), MAX_BB_BYTES);
                        byte[] bytes = new byte[len];
                        bb.get(bytes, 0, len);
                        StringBuilder sb = new StringBuilder("0x");
                        sb.append(Hex.encodeHexString(bytes));
                        if(threedots) sb.append("...");
                        System.out.println(sb.toString());
                    } else System.out.println(me.getKey().toString());
                }
            }
        }
    }

    public void toJSON(int maxRows) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        SimpleModule module = new SimpleModule("Bytes serializer", new Version(0,1,0, null, "pl.ato", "bin-ser"));
        module.addSerializer(Binary.class, new BinarySerializer(Binary.class));
        module.addSerializer(ByteBuffer.class, new ByteBufferSerializer(ByteBuffer.class));
        mapper.registerModule(module);
        int cnt = 0;
        try {
            for (Map<String, Object> row : this) {
                if (row != null) {
                    if (maxRows > 0 && maxRows <= cnt++) break;
                    System.out.println(mapper.writeValueAsString(row));
                }
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public void toCSV(String delimiter, boolean quote, int maxRows) {
        // print header
        int cnt = 0;
        if(delimiter == null) delimiter = ",";
        System.out.println(StringUtils.join(fields,delimiter));

        int noOfFields = fields.size();

        StringBuilder sb = new StringBuilder();
        for (Map<String, Object> row : this) {
            if (row != null) {
                if(maxRows > 0 && maxRows <= cnt++) break;
                for (int i = 0; i < noOfFields; i++) {
                    if(i>0) sb.append(delimiter);
                    String field = fields.get(i);
                    Object value = row.get(field);
                    if (value != null) {
                        if (javaTypes.get(i) == String.class) {
                            String sValue = value.toString();
                            if(quote) sb.append('"').append(sValue).append('"');
                            else sb.append(value);
                        } else if(value instanceof Binary) {
                            sb.append(Hex.encodeHexString(((Binary) value).getBytes()));
                        } else if(value instanceof ByteBuffer) {
                            // TODO: check .hasArray()
                            sb.append(Hex.encodeHexString(((ByteBuffer)value).array()));
                        } else {
                            sb.append(value);
                        }
                    }
                }
                System.out.println(sb.toString());
                sb.setLength(0);
            }
        }

    }

    public class InvalidParameterException extends Exception {

        public InvalidParameterException(String message) {
            super(message);
        }
    }

    /**
     * Use custom jackson serializers for ByteBuffer and org.apache.parquet.io.api.Binary
     */

    public static class BinarySerializer extends StdSerializer<Binary> {

        public BinarySerializer(Class<Binary> t) {
            super(t);
        }

        @Override
        public void serialize(Binary binary, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeString(Hex.encodeHexString(binary.getBytes()));
        }
    }

    public static class ByteBufferSerializer extends StdSerializer<ByteBuffer> {

        protected ByteBufferSerializer(Class<ByteBuffer> t) {
            super(t);
        }

        @Override
        public void serialize(ByteBuffer byteBuffer, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
            // TODO: check if byteBuffer.hasArray()
            jsonGenerator.writeString(Hex.encodeHexString(byteBuffer.array()));

        }
    }

}
