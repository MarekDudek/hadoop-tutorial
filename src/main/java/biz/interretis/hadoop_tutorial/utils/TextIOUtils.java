package biz.interretis.hadoop_tutorial.utils;

import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

public class TextIOUtils {

    public static void printLines(final String path) throws IOException {

        final URL file = new URL(path);

        try (final InputStream stream = file.openStream()) {

            final List<String> lines = org.apache.commons.io.IOUtils.readLines(stream);

            for(final String line: lines) {
                System.out.println(line);
            }
        }
    }

    public static void displayFromFilesystem(final String hdfsFile) throws IOException {

        try (final InputStream stream = new URL(hdfsFile).openStream()) {
            IOUtils.copyBytes(stream, System.out, 4096, true);
        }
    }
}
