update task_details
set
	message_id='{message_id}',
	sys_updated=now()
where
	task_id='{task_id}'