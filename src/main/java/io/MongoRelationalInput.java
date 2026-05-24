package io;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import runner.Config;
import runner.ConnectionRegistry.MongoConnection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Streams rows from a MongoDB collection.
 *
 * Schema is inferred by sampling up to {@value #SAMPLE_SIZE} documents and collecting the
 * union of dot-notation leaf paths. Nested objects flatten as {@code address.city}. Arrays
 * of scalars or sub-documents are exploded: one row per element. If a document contains
 * multiple array fields, only the lexicographically-first one (the "primary array") is
 * exploded; the others are serialized as their {@code toString()} form. This keeps row
 * counts bounded.
 */
public class MongoRelationalInput implements RelationalInput {

    private static final int SAMPLE_SIZE = 1000;

    private final Config config;
    private final MongoClient client;
    private final MongoCursor<Document> cursor;
    private final String[] header;
    private final String primaryArrayPath;

    private Iterator<String[]> bufferedRows = Collections.emptyIterator();

    public MongoRelationalInput(MongoConnection conn, String databaseName, String collectionName, Config config) {
        this.config = config;
        this.client = MongoClients.create(conn.uri());
        MongoCollection<Document> collection = client.getDatabase(databaseName).getCollection(collectionName);

        TreeSet<String> paths = new TreeSet<>();
        TreeSet<String> arrayPaths = new TreeSet<>();
        try (MongoCursor<Document> sample = collection.find().limit(SAMPLE_SIZE).iterator()) {
            while (sample.hasNext()) {
                collectPaths(sample.next(), "", paths, arrayPaths);
            }
        }

        this.primaryArrayPath = arrayPaths.isEmpty() ? null : arrayPaths.first();
        this.header = paths.toArray(new String[0]);

        this.cursor = collection.find().iterator();
        prefetch();
    }

    private static void collectPaths(Document doc, String prefix, TreeSet<String> paths, TreeSet<String> arrayPaths) {
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            String path = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Document subDoc) {
                collectPaths(subDoc, path, paths, arrayPaths);
            } else if (value instanceof List<?> list) {
                arrayPaths.add(path);
                paths.add(path);
                if (!list.isEmpty() && list.get(0) instanceof Document elemDoc) {
                    collectPaths(elemDoc, path, paths, arrayPaths);
                }
            } else {
                paths.add(path);
            }
        }
    }

    private void prefetch() {
        while (!bufferedRows.hasNext() && cursor.hasNext()) {
            bufferedRows = expand(cursor.next()).iterator();
        }
    }

    private List<String[]> expand(Document doc) {
        Map<String, String> base = new HashMap<>();
        Object primaryArrayValue = flatten(doc, "", base);

        if (primaryArrayValue instanceof List<?> list && !list.isEmpty()) {
            List<String[]> rows = new ArrayList<>(list.size());
            for (Object element : list) {
                Map<String, String> row = new HashMap<>(base);
                if (element instanceof Document elemDoc) {
                    flatten(elemDoc, primaryArrayPath, row);
                } else {
                    row.put(primaryArrayPath, element == null ? null : element.toString());
                }
                rows.add(toRow(row));
            }
            return rows;
        }
        return Collections.singletonList(toRow(base));
    }

    private Object flatten(Document doc, String prefix, Map<String, String> out) {
        Object primaryArrayValue = null;
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            String path = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Document subDoc) {
                Object nested = flatten(subDoc, path, out);
                if (primaryArrayValue == null) {
                    primaryArrayValue = nested;
                }
            } else if (value instanceof List<?> list) {
                if (path.equals(primaryArrayPath)) {
                    primaryArrayValue = list;
                } else {
                    out.put(path, list.toString());
                }
            } else {
                out.put(path, value == null ? null : value.toString());
            }
        }
        return primaryArrayValue;
    }

    private String[] toRow(Map<String, String> values) {
        String[] row = new String[header.length];
        for (int i = 0; i < header.length; i++) {
            String value = values.get(header[i]);
            row[i] = value == null ? config.nullString : value;
        }
        return row;
    }

    @Override
    public boolean hasNext() {
        return bufferedRows.hasNext();
    }

    @Override
    public String[] next() {
        String[] current = bufferedRows.next();
        if (!bufferedRows.hasNext()) {
            prefetch();
        }
        return current;
    }

    @Override
    public String[] getHeader() {
        return header;
    }

    @Override
    public void close() throws IOException {
        cursor.close();
        client.close();
    }
}
