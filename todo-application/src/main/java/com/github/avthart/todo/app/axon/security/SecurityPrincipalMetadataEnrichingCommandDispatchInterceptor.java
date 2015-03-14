package com.github.avthart.todo.app.axon.security;

import java.util.Collections;

import org.axonframework.commandhandling.CommandDispatchInterceptor;
import org.axonframework.commandhandling.CommandMessage;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

public class SecurityPrincipalMetadataEnrichingCommandDispatchInterceptor implements CommandDispatchInterceptor {

	@Override
	public CommandMessage<?> handle(CommandMessage<?> commandMessage) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null) {
        	return commandMessage;
        }
        
		return commandMessage.andMetaData(Collections.singletonMap("authentication", authentication));
	}

}
