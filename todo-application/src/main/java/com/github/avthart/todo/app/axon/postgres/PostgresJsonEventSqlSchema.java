package com.github.avthart.todo.app.axon.config;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.axonframework.eventstore.jdbc.GenericEventSqlSchema;
import org.axonframework.eventstore.jdbc.SchemaConfiguration;
import org.joda.time.DateTime;

public class PostgresJsonEventSqlSchema<T> extends GenericEventSqlSchema<T> {

    public PostgresJsonEventSqlSchema() {
    }

    public PostgresJsonEventSqlSchema(Class<T> dataType) {
        super(dataType);
    }

    public PostgresJsonEventSqlSchema(Class<T> dataType, SchemaConfiguration schemaConfiguration) {
        super(dataType, schemaConfiguration);
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
            T eventPayload, T eventMetaData, String aggregateType) throws SQLException {
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
}