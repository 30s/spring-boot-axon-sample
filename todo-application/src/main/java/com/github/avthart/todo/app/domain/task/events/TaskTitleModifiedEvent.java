package com.github.avthart.todo.app.domain.task.events;

import lombok.Value;

/**
 * @author albert
 */
@Value
public class TaskTitleModifiedEvent implements TaskEvent {

	private final String id;
	
	private final String title;
}
