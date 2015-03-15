package com.github.avthart.todo.app.web.task.requests;

import javax.validation.constraints.NotNull;

/**
 * @author albert
 */
public class ModifyTitleRequest {
	
	@NotNull
	private String title;
	
	public void setTitle(String title) {
		this.title = title;
	}
	
	public String getTitle() {
		return title;
	}
}
