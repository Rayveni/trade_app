update task_status
set
	status='{task_status}',
	error_message={error_message},
	sys_updated=now()
where
	task_id='{task_id}'