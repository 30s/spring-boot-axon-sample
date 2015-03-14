package com.github.avthart.todo.app.axon.security;

import java.util.Collections;
import java.util.Map;

import org.axonframework.auditing.AuditDataProvider;
import org.axonframework.commandhandling.CommandMessage;
import org.springframework.security.core.Authentication;

public class SpringSecurityAuditDataProvider implements AuditDataProvider {

    @Override
    public Map<String, Object> provideAuditDataFor(CommandMessage<?> command) {
        Object currentUser = command.getMetaData().get("authentication");
        if (currentUser != null && currentUser instanceof Authentication) {
            return Collections.<String, Object>singletonMap("principal", ((Authentication) currentUser).getName());
        }
        return Collections.emptyMap();
    }

}
