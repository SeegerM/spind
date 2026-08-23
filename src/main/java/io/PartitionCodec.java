package io;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Binary encoding for partition spill records.
 *
 * <p>One record per distinct value:</p>
 *
 * <pre>
 *   int    value length in UTF-8 bytes
 *   byte[] value bytes
 *   int    number of attributes
 *   (int attributeId, long occurrences) * number of attributes
 * </pre>
 *
 * <p>The text format this replaces spent its time formatting integers into decimal, scanning for
 * {@code ,} and {@code ;}, allocating a String per line and parsing the digits back. None of that
 * is inherent to the data — partition write and load together were over half of a large run. Values
 * are length-prefixed rather than newline-delimited, which also removes the need to escape values
 * containing newlines, something the text format quietly relied on the source never producing.</p>
 *
 * <p>Fixed-width integers on purpose: variable-length encoding would shrink the files further, but
 * the point of the first version is to find out how much of the cost was text handling rather than
 * bytes moved.</p>
 */
final class PartitionCodec {

    private static final int BUFFER_BYTES = 1 << 16;

    private PartitionCodec() {
    }

    static Writer writer(Path path) throws IOException {
        return new Writer(path);
    }

    static final class Writer implements AutoCloseable {

        private final DataOutputStream out;

        private Writer(Path path) throws IOException {
            OutputStream stream = Files.newOutputStream(path,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            this.out = new DataOutputStream(new BufferedOutputStream(stream, BUFFER_BYTES));
        }

        void write(String value, ValueEntry entry) throws IOException {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            out.writeInt(bytes.length);
            out.write(bytes);
            out.writeInt(entry.size);
            for (int i = 0; i < entry.size; i++) {
                out.writeInt(entry.ids[i]);
                out.writeLong(entry.occurrences[i]);
            }
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    /** Reads records back, handing each to the consumer. Returns when the file is exhausted. */
    static void read(Path path, RecordConsumer consumer) throws IOException {
        try (InputStream stream = Files.newInputStream(path);
             DataInputStream in = new DataInputStream(new BufferedInputStream(stream, BUFFER_BYTES))) {
            byte[] scratch = new byte[256];
            while (true) {
                int length;
                try {
                    length = in.readInt();
                } catch (EOFException e) {
                    return;
                }
                if (scratch.length < length) {
                    scratch = new byte[Math.max(length, scratch.length * 2)];
                }
                in.readFully(scratch, 0, length);
                String value = new String(scratch, 0, length, StandardCharsets.UTF_8);

                int attributes = in.readInt();
                consumer.begin(value, attributes);
                for (int i = 0; i < attributes; i++) {
                    consumer.attribute(in.readInt(), in.readLong());
                }
            }
        }
    }

    interface RecordConsumer {
        /** Called once per record, before its attributes. */
        void begin(String value, int attributeCount);

        /** Called once per (attribute, occurrences) pair of the current record. */
        void attribute(int attributeId, long occurrences);
    }
}
