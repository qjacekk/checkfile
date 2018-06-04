package pl.ato;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;

public class TestBase {

    protected ByteArrayOutputStream out;
    private PrintStream systemOut;

    protected void captureOutput() {
        out = new ByteArrayOutputStream();
        systemOut = System.out;
        System.setOut(new PrintStream(out));
    }
    protected void restoreOutput() throws IOException {
        if(systemOut != null) System.setOut(systemOut);
        if(out != null) out.close();
    }

    protected File getFileResource(String name) {
        ClassLoader cl = getClass().getClassLoader();
        URL resource = cl.getResource(name);
        if(resource == null) throw new NullPointerException("Missing resource: " + name );
        return new File(resource.getFile());
    }

}
