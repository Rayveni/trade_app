CREATE TABLE IF NOT EXISTS task_status (
  task_id VARCHAR(36) PRIMARY KEY,
  parent_task_id VARCHAR(36)  NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'NEW',
  error_message VARCHAR(56) NULL,
  retry_count int NULL,
  sys_created TIMESTAMP DEFAULT now (),
  sys_updated TIMESTAMP NULL
);
CREATE INDEX IF NOT EXISTS task_status_idx ON task_status (parent_task_id);

CREATE TABLE IF NOT EXISTS task_details (
  task_id VARCHAR(36) PRIMARY KEY,
  topic VARCHAR(20) NULL,
  message_id VARCHAR(20) NOT NULL,
  message json NOT NULL,
  header json NOT NULL,
  sys_created TIMESTAMP DEFAULT now (),
  sys_updated TIMESTAMP NULL
);



