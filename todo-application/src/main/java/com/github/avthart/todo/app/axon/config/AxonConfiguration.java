package com.github.avthart.todo.app.axon.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import lombok.extern.slf4j.Slf4j;

import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.CommandDispatchInterceptor;
import org.axonframework.commandhandling.CommandHandlerInterceptor;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.commandhandling.annotation.AggregateAnnotationCommandHandler;
import org.axonframework.commandhandling.annotation.AnnotationCommandHandlerBeanPostProcessor;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.commandhandling.gateway.CommandGatewayFactoryBean;
import org.axonframework.commandhandling.interceptors.BeanValidationInterceptor;
import org.axonframework.commandhandling.interceptors.LoggingInterceptor;
import org.axonframework.domain.AggregateRoot;
import org.axonframework.domain.EventContainer;
import org.axonframework.eventhandling.Cluster;
import org.axonframework.eventhandling.ClusteringEventBus;
import org.axonframework.eventhandling.DefaultClusterSelector;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.EventBusTerminal;
import org.axonframework.eventhandling.SimpleCluster;
import org.axonframework.eventhandling.SimpleClusterFactoryBean;
import org.axonframework.eventhandling.amqp.spring.ListenerContainerLifecycleManager;
import org.axonframework.eventhandling.amqp.spring.SpringAMQPConsumerConfiguration;
import org.axonframework.eventhandling.amqp.spring.SpringAMQPTerminal;
import org.axonframework.eventhandling.annotation.AnnotationEventListenerBeanPostProcessor;
import org.axonframework.eventsourcing.AggregateFactory;
import org.axonframework.eventsourcing.EventCountSnapshotterTrigger;
import org.axonframework.eventsourcing.EventSourcedAggregateRoot;
import org.axonframework.eventsourcing.EventSourcingRepository;
import org.axonframework.eventsourcing.GenericAggregateFactory;
import org.axonframework.eventsourcing.SnapshotterTrigger;
import org.axonframework.eventsourcing.SpringAggregateSnapshotter;
import org.axonframework.eventstore.jdbc.JdbcEventStore;
import org.axonframework.repository.AbstractRepository;
import org.axonframework.repository.Repository;
import org.axonframework.serializer.Serializer;
import org.axonframework.serializer.json.JacksonSerializer;
import org.axonframework.unitofwork.SpringTransactionManager;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ErrorHandler;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.github.avthart.todo.app.axon.security.SecurityPrincipalMetadataEnrichingCommandDispatchInterceptor;
import com.github.avthart.todo.app.support.ClassScanner;
import com.github.avthart.todo.app.support.ConstructorPropertiesAnnotationIntrospector;

@Configuration
@ConditionalOnClass({ AggregateRoot.class, Repository.class })
@Slf4j
public class AxonConfiguration implements BeanFactoryAware {
    
    private ConfigurableListableBeanFactory beanFactory;
    
    @Autowired
    private PlatformTransactionManager transactionManager;
    
    @Autowired
    private ConnectionFactory connectionFactory;
    
    @Autowired
    private Serializer serializer;
    
    @Autowired
    private CommandBus commandBus;
    
    @Autowired
    private SpringAggregateSnapshotter springAggregateSnapshotter;

    @Value("${axon.eventstore.createSnapshotEvery:10}")
	private int createSnapshotEvery;
    
    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
    }

	@Bean
	@ConditionalOnMissingBean(AnnotationCommandHandlerBeanPostProcessor.class)
	public AnnotationEventListenerBeanPostProcessor annotationEventListenerBeanPostProcessor(EventBus eventBus) {
		AnnotationEventListenerBeanPostProcessor processor = new AnnotationEventListenerBeanPostProcessor();
		processor.setEventBus(eventBus);
		return processor;
	}
	
	@Bean
	@ConditionalOnMissingBean(AnnotationCommandHandlerBeanPostProcessor.class)
	public AnnotationCommandHandlerBeanPostProcessor annotationCommandHandlerBeanPostProcessor(CommandBus commandBus) {
		AnnotationCommandHandlerBeanPostProcessor processor = new AnnotationCommandHandlerBeanPostProcessor();
		processor.setCommandBus(commandBus);
		return processor;
	}

	@Bean
	@ConditionalOnMissingBean(CommandGatewayFactoryBean.class)
	public CommandGatewayFactoryBean<CommandGateway> commandGatewayFactoryBean(CommandBus commandBus) {
		CommandGatewayFactoryBean<CommandGateway> factory = new CommandGatewayFactoryBean<CommandGateway>();
		factory.setCommandBus(commandBus);
		factory.setCommandDispatchInterceptors(beanValidationInterceptor(), securityPrincipalMetadataInterceptor());
		return factory;
	}
	
	@Bean
	@ConditionalOnMissingBean(EventBus.class)
	public EventBus eventBus() {
		return new ClusteringEventBus(new DefaultClusterSelector(cluster()), eventBusTerminal());
	}

	@Bean
	Queue queue() {
		return new Queue("Axon.EventBus.Default", false);
	}

	@Bean
	FanoutExchange exchange() {
		return new FanoutExchange("Axon.EventBus");
	}
	
	@Bean
	Binding binding() {
		return BindingBuilder.bind(queue()).to(exchange());
	}

	@Bean
	EventBusTerminal eventBusTerminal() {
		SpringAMQPTerminal terminal = new SpringAMQPTerminal();
		terminal.setConnectionFactory(connectionFactory);
		terminal.setSerializer(serializer);
		terminal.setExchange(exchange());
		terminal.setListenerContainerLifecycleManager(listenerContainerLifecycleManager());
		return terminal;
	}
	
	@Bean
	Cluster cluster() {
		SimpleCluster cluster = new SimpleCluster("axon-cluster");
		SpringAMQPConsumerConfiguration config = new SpringAMQPConsumerConfiguration();
		config.setQueueName("Axon.EventBus.Default");
		cluster.getMetaData().setProperty("AMQP.Config", config);
		return cluster;
	}
	
	@Bean
	// TODO check config params..
	SpringAMQPConsumerConfiguration amqpConsumerConfiguration() {
		SpringAMQPConsumerConfiguration config = new SpringAMQPConsumerConfiguration();
		config.setTransactionManager(transactionManager);
		config.setTxSize(25);
		config.setPrefetchCount(200);
		config.setShutdownTimeout(10L);
		config.setQueueName("Axon.EventBus.Default");
		config.setErrorHandler(new ErrorHandler() {
			@Override
			public void handleError(Throwable error) {
				log.error("Exception occured in Axon AMQP handler", error);
			}
		});
		return config;
	}
	
	@Bean
	public ListenerContainerLifecycleManager listenerContainerLifecycleManager() {
		ListenerContainerLifecycleManager manager = new ListenerContainerLifecycleManager();
		manager.setDefaultConfiguration(amqpConsumerConfiguration());
		manager.setConnectionFactory(connectionFactory);
		return manager;
	}

	@Bean
	SpringTransactionManager springTransactionManager() {
		return new SpringTransactionManager(transactionManager);
	}
	
	@Bean
	@ConditionalOnMissingBean(CommandBus.class)
	public CommandBus commandBus(List<CommandHandlerInterceptor> interceptors, SpringTransactionManager springTransactionManager) {
		SimpleCommandBus commandBus = new SimpleCommandBus();
		commandBus.setHandlerInterceptors(interceptors);
		commandBus.setTransactionManager(springTransactionManager);
		return commandBus;
	}
	
    @Bean
    @ConditionalOnMissingBean(LoggingInterceptor.class)
    public LoggingInterceptor loggingInterceptor() {
        LoggingInterceptor interceptor = new LoggingInterceptor();
        return interceptor;
    }
	
	@Bean
	@ConditionalOnMissingBean(BeanValidationInterceptor.class)
	public BeanValidationInterceptor beanValidationInterceptor() {
	    BeanValidationInterceptor interceptor = new BeanValidationInterceptor();
	    return interceptor;
	}

	@Bean
	@ConditionalOnMissingBean(SecurityPrincipalMetadataEnrichingCommandDispatchInterceptor.class)
	public CommandDispatchInterceptor securityPrincipalMetadataInterceptor() {
		SecurityPrincipalMetadataEnrichingCommandDispatchInterceptor interceptor = new SecurityPrincipalMetadataEnrichingCommandDispatchInterceptor();
	    return interceptor;
	}

	
	@SuppressWarnings("unchecked")
	@Bean
	public Collection<Class<? extends EventSourcedAggregateRoot<?>>> aggregateRoots() {
		ClassScanner scanner = new ClassScanner(getPackagesToScan()).withAssignableFilter(EventSourcedAggregateRoot.class);
		Collection<Class<? extends EventSourcedAggregateRoot<?>>> r = new ArrayList<Class<? extends EventSourcedAggregateRoot<?>>>();
		for (Class<?> c : scanner.findClasses()) {
			r.add((Class<? extends EventSourcedAggregateRoot<?>>) c);
		}
		return r;
	}
	
    @Bean
    public List<AbstractRepository<?>> repositories(@Value("#{aggregateRoots}")  Collection<Class<? extends EventSourcedAggregateRoot<?>>> aggregateRoots) {
		ArrayList<AbstractRepository<?>> repositories = new ArrayList<AbstractRepository<?>>();
        for (Class<? extends EventSourcedAggregateRoot<?>> clazz : aggregateRoots) {
            log.info("Created EventSourcing Repository for aggregate {}", clazz.getName());
            @SuppressWarnings({ "unchecked", "rawtypes" })
			EventSourcingRepository<?> repository = new EventSourcingRepository(clazz, getJdbcEventStore());
        	repository.setSnapshotterTrigger(snapshotterTrigger());
        	
            repository.setEventBus(eventBus());
            repositories.add(repository);

            @SuppressWarnings({ "rawtypes", "unchecked" })
			AggregateAnnotationCommandHandler<?> commandHandler = new AggregateAnnotationCommandHandler(clazz, repository);
            Set<?> commands = commandHandler.supportedCommands();
            for (Object commandName : commands) {
                commandBus.subscribe(commandName.toString(), commandHandler);
                log.info("Subscibed command handler {}", commandName.toString());
            }
        }
        return repositories;
    }

    private JdbcEventStore getJdbcEventStore() {
        return beanFactory.getBean(JdbcEventStore.class);
    }
    
	@Bean
	public SnapshotterTrigger snapshotterTrigger() {
		EventCountSnapshotterTrigger trigger = new EventCountSnapshotterTrigger();
		trigger.setTrigger(createSnapshotEvery);
		trigger.setSnapshotter(springAggregateSnapshotter);
		return trigger;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Bean
	public SpringAggregateSnapshotter springAggregateSnapshotter(@Value("#{aggregateRoots}") Collection<Class<? extends EventSourcedAggregateRoot<?>>> aggregateRoots, PlatformTransactionManager platformTransactionManager) {
		List<AggregateFactory<?>> factories = new ArrayList<AggregateFactory<?>>(aggregateRoots.size());
		for (Class<?> aggregateRoot : aggregateRoots) {
			factories.add(new GenericAggregateFactory(aggregateRoot));
		}
		
		SpringAggregateSnapshotter snapshotter = new SpringAggregateSnapshotter();
		snapshotter.setExecutor(executor());
		snapshotter.setAggregateFactories(factories);
		snapshotter.setEventStore(getJdbcEventStore());
		snapshotter.setTransactionManager(platformTransactionManager);
		return snapshotter;
	}
	
	@Bean(destroyMethod="shutdown")
	public Executor executor() {
		return Executors.newScheduledThreadPool(5); // TODO make a shared executor make poolsize configurable.
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

    private String[] getPackagesToScan() {
        List<String> basePackages = AutoConfigurationPackages.get(this.beanFactory);
        return basePackages.toArray(new String[basePackages.size()]);
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
}