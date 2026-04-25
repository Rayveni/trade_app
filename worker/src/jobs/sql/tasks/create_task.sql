BEGIN;
INSERT INTO task_status (task_id)
VALUES {task_status_values}
ON CONFLICT (task_id) 
DO UPDATE 
SET status = 'NEW',
    error_message = null,
	sys_updated=now()
	;
INSERT INTO task_details (task_id,topic,message_id,message,header)
VALUES {task_details_values}
ON CONFLICT (task_id) 
DO UPDATE 
SET  topic = EXCLUDED.topic,
	message_id = EXCLUDED.message_id,
	header = EXCLUDED.header,
	sys_updated=now()
	;
COMMIT;