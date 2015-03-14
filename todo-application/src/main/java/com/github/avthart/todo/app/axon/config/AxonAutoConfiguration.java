package com.github.avthart.todo.app.axon.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

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
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.SimpleEventBus;
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
import org.axonframework.unitofwork.SpringTransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import com.github.avthart.todo.app.axon.security.SecurityPrincipalMetadataEnrichingCommandDispatchInterceptor;
import com.github.avthart.todo.app.support.ClassScanner;

@Configuration
@ConditionalOnClass({ AggregateRoot.class, Repository.class })
public class AxonAutoConfiguration implements BeanFactoryAware {
	
    private static final Logger LOGGER = LoggerFactory.getLogger(AxonAutoConfiguration.class);
    
    private ConfigurableListableBeanFactory beanFactory;
    
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
		return new SimpleEventBus();
	}
	
	@Bean
	SpringTransactionManager springTransactionManager(PlatformTransactionManager platformTransactionManager) {
		return new SpringTransactionManager(platformTransactionManager);
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
            LOGGER.info("Created EventSourcing Repository for aggregate {}", clazz.getName());
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
                LOGGER.info("Subscibed command handler {}", commandName.toString());
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
    
    private String[] getPackagesToScan() {
        List<String> basePackages = AutoConfigurationPackages.get(this.beanFactory);
        return basePackages.toArray(new String[basePackages.size()]);
    }
}