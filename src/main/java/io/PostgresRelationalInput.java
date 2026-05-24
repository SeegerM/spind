package io;

import runner.Config;
import runner.ConnectionRegistry.PostgresConnection;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Streams rows from a PostgreSQL table via a server-side cursor.
 *
 * Uses {@code setAutoCommit(false)} + {@code setFetchSize} to avoid loading the entire result
 * set into JVM memory, which is required by the Postgres JDBC driver to enable streaming.
 */
public class PostgresRelationalInput implements RelationalInput {

    private static final int FETCH_SIZE = 1000;

    private final Config config;
    private final Connection connection;
    private final PreparedStatement statement;
    private final ResultSet resultSet;
    private final String[] header;
    private final int columnCount;

    private String[] nextRow;

    public PostgresRelationalInput(PostgresConnection conn, String schema, String table, Config config) throws SQLException {
        this.config = config;
        this.connection = DriverManager.getConnection(conn.url(), conn.user(), conn.password());
        this.connection.setAutoCommit(false);

        String sql = "SELECT * FROM " + quote(schema) + "." + quote(table);
        this.statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        this.statement.setFetchSize(FETCH_SIZE);
        this.resultSet = statement.executeQuery();

        ResultSetMetaData md = resultSet.getMetaData();
        this.columnCount = md.getColumnCount();
        this.header = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            header[i] = md.getColumnLabel(i + 1);
        }

        advance();
    }

    private void advance() {
        try {
            if (!resultSet.next()) {
                nextRow = null;
                return;
            }
            String[] row = new String[columnCount];
            for (int i = 0; i < columnCount; i++) {
                Object value = resultSet.getObject(i + 1);
                if (resultSet.wasNull() || value == null) {
                    row[i] = config.nullString;
                } else {
                    row[i] = value.toString();
                }
            }
            nextRow = row;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read row from Postgres", e);
        }
    }

    @Override
    public boolean hasNext() {
        return nextRow != null;
    }

    @Override
    public String[] next() {
        String[] current = nextRow;
        advance();
        return current;
    }

    @Override
    public String[] getHeader() {
        return header;
    }

    @Override
    public void close() throws IOException {
        try {
            if (resultSet != null) resultSet.close();
            if (statement != null) statement.close();
            if (connection != null) connection.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
