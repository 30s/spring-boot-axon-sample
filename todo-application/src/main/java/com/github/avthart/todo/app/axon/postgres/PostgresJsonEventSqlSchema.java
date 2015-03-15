package com.github.avthart.todo.app.axon.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.axonframework.eventstore.jdbc.GenericEventSqlSchema;
import org.axonframework.eventstore.jdbc.SchemaConfiguration;
import org.joda.time.DateTime;

public class PostgresJsonEventSqlSchema extends GenericEventSqlSchema<String> {
	
	private static final String STD_FIELDS = "eventIdentifier, aggregateIdentifier, sequenceNumber, timeStamp, "
            + "payloadType, payloadRevision, payload::jsonb, metaData::jsonb";
	
	public PostgresJsonEventSqlSchema() {
		super(String.class);
	}
	
    public PostgresJsonEventSqlSchema(SchemaConfiguration schemaConfiguration) {
        super(String.class, schemaConfiguration);
    }

    @Override
    public PreparedStatement sql_createSnapshotEventEntryTable(Connection connection) throws SQLException {
        final String sql = "create table " + schemaConfiguration.snapshotEntryTable() + " (" +
                "        aggregateIdentifier varchar(255) not null," +
                "        sequenceNumber bigint not null," +
                "        type varchar(255) not null," +
                "        eventIdentifier varchar(255) not null," +
                "        metaData jsonb," +
                "        payload jsonb not null," +
                "        payloadRevision varchar(255)," +
                "        payloadType varchar(255) not null," +
                "        timeStamp varchar(255) not null," +
                "        primary key (aggregateIdentifier, sequenceNumber, type)" +
                "    );";
        return connection.prepareStatement(sql);
    }
    
    @Override
    public PreparedStatement sql_createDomainEventEntryTable(Connection connection) throws SQLException {
        final String sql = "create table " + schemaConfiguration.domainEventEntryTable() + " (" +
                "        aggregateIdentifier varchar(255) not null," +
                "        sequenceNumber bigint not null," +
                "        type varchar(255) not null," +
                "        eventIdentifier varchar(255) not null," +
                "        metaData jsonb," +
                "        payload jsonb not null," +
                "        payloadRevision varchar(255)," +
                "        payloadType varchar(255) not null," +
                "        timeStamp varchar(255) not null," +
                "        primary key (aggregateIdentifier, sequenceNumber, type)" +
                "    );";
        return connection.prepareStatement(sql);
    }
    
    protected PreparedStatement doInsertEventEntry(String tableName, Connection connection, String eventIdentifier,
            String aggregateIdentifier,
            long sequenceNumber, DateTime timestamp, String eventType,
            String eventRevision,
            String eventPayload, String eventMetaData, String aggregateType) throws SQLException {
		final String sql = "INSERT INTO " + tableName
		+ " (eventIdentifier, type, aggregateIdentifier, sequenceNumber, timeStamp, payloadType, "
		+ "payloadRevision, payload, metaData) VALUES (?,?,?,?,?,?,?,?::jsonb,?::jsonb)";
		PreparedStatement preparedStatement = connection.prepareStatement(sql); // NOSONAR
		preparedStatement.setString(1, eventIdentifier);
		preparedStatement.setString(2, aggregateType);
		preparedStatement.setString(3, aggregateIdentifier);
		preparedStatement.setLong(4, sequenceNumber);
		preparedStatement.setString(5, sql_dateTime(timestamp));
		preparedStatement.setString(6, eventType);
		preparedStatement.setString(7, eventRevision);
		preparedStatement.setObject(8, eventPayload);
		preparedStatement.setObject(9, eventMetaData);
		return preparedStatement;	
	}
    
    @Override
    public PreparedStatement sql_loadLastSnapshot(Connection connection, Object identifier, String aggregateType)
            throws SQLException {
        final String s = "SELECT " + STD_FIELDS + " FROM " + schemaConfiguration.snapshotEntryTable()
                + " WHERE aggregateIdentifier = ? AND type = ? ORDER BY sequenceNumber DESC";
        PreparedStatement statement = connection.prepareStatement(s);
        statement.setString(1, identifier.toString());
        statement.setString(2, aggregateType);
        return statement;
    }

    @Override
    public PreparedStatement sql_fetchFromSequenceNumber(Connection connection, String type, Object aggregateIdentifier,
                                                         long firstSequenceNumber) throws SQLException {
        final String sql = "SELECT " + STD_FIELDS + " FROM " + schemaConfiguration.domainEventEntryTable()
                + " WHERE aggregateIdentifier = ? AND type = ?"
                + " AND sequenceNumber >= ?"
                + " ORDER BY sequenceNumber ASC";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, aggregateIdentifier.toString());
        preparedStatement.setString(2, type);
        preparedStatement.setLong(3, firstSequenceNumber);
        return preparedStatement;
    }

    @Override
    public PreparedStatement sql_getFetchAll(Connection connection, String whereClause,
                                             Object[] params) throws SQLException {
        final String sql = "select " + STD_FIELDS + " from " + schemaConfiguration.domainEventEntryTable()
                + " e " + whereClause
                + " ORDER BY e.timeStamp ASC, e.sequenceNumber ASC, e.aggregateIdentifier ASC ";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        for (int i = 0; i < params.length; i++) {
            Object param = params[i];
            if (param instanceof DateTime) {
                param = sql_dateTime((DateTime) param);
            }

            if (param instanceof byte[]) {
                preparedStatement.setBytes(i + 1, (byte[]) param);
            } else {
                preparedStatement.setObject(i + 1, param);
            }
        }
        return preparedStatement;
    }
    
	@Override
    protected String readPayload(ResultSet resultSet, int columnIndex) throws SQLException {
    	// always read as String
        return resultSet.getString(columnIndex);
    }
}