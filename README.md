# Trade app
**important** :use api to create queue topics

## Concept diagram
```mermaid
sequenceDiagram
    actor User
    participant Front
    participant front_task_queue@{ "type" : "queue" }
    participant workers_group1
    participant tasks_bd@{ "type" : "database" }
	participant back_task_queue@{ "type" : "queue" }
    User->>Front: Create task
    Front->>front_task_queue :task message
    front_task_queue->>workers_group1: receive task message
	workers_group1-->>tasks_bd:write task status & description
    workers_group1->>back_task_queue:task message
	back_task_queue->>workers_group2:receive task message
	workers_group2->>workers_group2:process task(create subtasks if need)
	workers_group2->>tasks_bd:update task status
    User->>Front: current task status?
    Front ->> tasks_bd:request
    tasks_bd-->>Front:response
