package com.github.avthart.todo.app.axon.security;

import org.axonframework.auditing.AuditLogger;
import org.axonframework.auditing.AuditingInterceptor;
import org.axonframework.auditing.NullAuditLogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.Authentication;

@Configuration
@ConditionalOnClass({ AuditingInterceptor.class, Authentication.class })
public class AxonSpringSecurityAutoConfiguration {

    @Autowired
    private AuditLogger auditLogger;
    
    @Bean
    @ConditionalOnMissingBean(AuditingInterceptor.class)
    public AuditingInterceptor auditingInterceptor() {
        AuditingInterceptor interceptor = new AuditingInterceptor();
        interceptor.setAuditDataProvider(new SpringSecurityAuditDataProvider());
        interceptor.setAuditLogger(auditLogger);
        return interceptor;
    }

    @Bean
    @ConditionalOnMissingBean(AuditLogger.class)
    public AuditLogger auditLogger() {
        return new NullAuditLogger();
    }    
}
