package com.github.avthart.todo.app.axon.config;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventsourcing.EventSourcedAggregateRoot;
import org.axonframework.eventstore.EventStoreException;
import org.axonframework.eventstore.jdbc.DefaultEventEntryStore;
import org.axonframework.eventstore.jdbc.EventSqlSchema;
import org.axonframework.eventstore.jdbc.JdbcEventStore;
import org.axonframework.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.github.avthart.todo.app.axon.postgres.PostgresJsonEventSqlSchema;

@Configuration
@ConditionalOnClass({ EventSourcedAggregateRoot.class, EnableTransactionManagement.class })
@ConditionalOnExpression("${axon.eventstore.jdbc.enabled:true}")
@AutoConfigureAfter(DataSourceAutoConfiguration.class)
public class AxonEventStoreJdbcConfiguration {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AxonEventStoreJdbcConfiguration.class);
    
    @Autowired
    private EventBus eventBus;
    
    @Autowired
    private DataSource dataSource;
    
    @Autowired
    private EventSqlSchema<String> eventSqlSchema;
    
    @Autowired
    private Serializer serializer;
    
    @Autowired(required = false)
    private PersistenceExceptionResolver persistenceExceptionResolver;
    
    @Value("${axon.eventstore.jdbc.batchSize:100}")
    private int batchSize;
    
    @Value("${axon.eventstore.jdbc.createSchema:false}")
    private boolean createSchema;
    
    @Bean
    @ConditionalOnMissingBean(EventSqlSchema.class)
    public EventSqlSchema<String> eventSqlSchema() {
		return new PostgresJsonEventSqlSchema();
    }
    
    @Bean
    @ConditionalOnMissingBean(JdbcEventStore.class)
    public JdbcEventStore jdbcEventStore() {
        DefaultEventEntryStore<String> eventEntryStore = new DefaultEventEntryStore<String>(dataSource, eventSqlSchema);
        JdbcEventStore eventStore = new JdbcEventStore(eventEntryStore, serializer);
        eventStore.setBatchSize(batchSize);
        if (persistenceExceptionResolver != null) {
            eventStore.setPersistenceExceptionResolver(persistenceExceptionResolver);
        }
        if(createSchema) {
            try {
                eventEntryStore.createSchema();
                LOGGER.debug("EventStore schema created");
            } catch (EventStoreException e) {
                LOGGER.warn("Error while creating eventStore schema (already created?): {}", e.getMessage());
            } catch (SQLException e) {
                LOGGER.warn("SQL error while creating eventStore schema", e);
            }            
        }
        return eventStore;
    }
}
