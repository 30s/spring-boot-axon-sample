package com.github.avthart.todo.app.notify.task;

import com.github.avthart.todo.app.domain.task.events.TaskEvent;

import lombok.Value;

@Value
public class TaskEventNotification {
	
	private String type;
	
	private TaskEvent data;
}
