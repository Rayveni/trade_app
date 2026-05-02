BEGIN;
INSERT INTO task_status (task_id,parent_task_id,status)
VALUES {task_status_values}
	;
INSERT INTO task_details (task_id,topic,message_id,message,header)
VALUES {task_details_values}
	;
COMMIT;