package com.github.avthart.todo.app.axon.config;

import java.sql.SQLException;
import java.util.Collection;

import javax.sql.DataSource;

import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.domain.EventContainer;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventsourcing.EventSourcedAggregateRoot;
import org.axonframework.eventstore.EventStoreException;
import org.axonframework.eventstore.jdbc.DefaultEventEntryStore;
import org.axonframework.eventstore.jdbc.EventSqlSchema;
import org.axonframework.eventstore.jdbc.JdbcEventStore;
import org.axonframework.serializer.Serializer;
import org.axonframework.serializer.json.JacksonSerializer;
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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.github.avthart.todo.app.axon.postgres.PostgresJsonEventSqlSchema;
import com.github.avthart.todo.app.support.ConstructorPropertiesAnnotationIntrospector;

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
    
    @Value("${axon.eventstore.jdbc.createSchema:true}")
    private boolean createSchema;
    
    @Bean
    @ConditionalOnMissingBean(EventSqlSchema.class)
    public EventSqlSchema<String> eventSqlSchema() {
		return new PostgresJsonEventSqlSchema();
    }
    
    @JsonAutoDetect(
    		fieldVisibility=Visibility.ANY,
    		getterVisibility=Visibility.NONE,
    		isGetterVisibility=Visibility.NONE,
    		setterVisibility=Visibility.NONE,
    		creatorVisibility=Visibility.ANY
    )
    private static abstract class AggregateRootSnapshotMixIn {
    	@JsonIgnore
        private volatile EventContainer eventContainer;

    	@JsonInclude(Include.NON_DEFAULT)
        private boolean deleted;
    }
    
    @Bean
    @ConditionalOnMissingBean(Serializer.class)
    public Serializer serializer(@Value("#{aggregateRoots}")  Collection<Class<? extends EventSourcedAggregateRoot<?>>> aggregateRoots) {
    	ObjectMapper mapper = new ObjectMapper();
    	
    	//
    	// Configure json serialization for AggregateRoots by applying mixins so we don't have to up any json annotations into the AR
    	//
    	for (Class<?> aggregateRootType : aggregateRoots) {
    		mapper.addMixInAnnotations(aggregateRootType, AggregateRootSnapshotMixIn.class);
    	}
    	
    	//
    	// Make jackson work with lombok @Value beans, @Value beans add @java.beans.ContructorProperties) to the generator code.
    	// The ConstructorPropertiesAnnotationIntrospector tells jackson how to create the bean.
    	//
    	mapper.setAnnotationIntrospector(AnnotationIntrospectorPair.create(/*new IgnoreTransientAnnotationIntrospector()*/ new JacksonAnnotationIntrospector(), new ConstructorPropertiesAnnotationIntrospector()));
    	
    	// An event could have no properties at all, make sure jackson accepts empty beans.
    	mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    	
        return new JacksonSerializer(mapper);
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
