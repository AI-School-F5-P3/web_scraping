import json
import time
import uuid

class Task:
    def __init__(self, company_id=None, company_data=None, task_id=None, worker_id=None):
        self.task_id = task_id or str(uuid.uuid4())
        self.company_id = company_id
        self.company_data = company_data
        self.worker_id = worker_id
        self.created_at = time.time()
        self.started_at = None
        self.completed_at = None
        self.status = "pending"  # pending, processing, completed, failed
        self.result = None
        self.error = None
    
    def to_json(self):
        return json.dumps({
            "task_id": self.task_id,
            "company_id": self.company_id,
            "company_data": self.company_data,
            "worker_id": self.worker_id,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "status": self.status,
            "result": self.result,
            "error": self.error
        })
    
    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        task = cls(
            company_id=data.get("company_id"),
            company_data=data.get("company_data"),
            task_id=data.get("task_id"),
            worker_id=data.get("worker_id")
        )
        task.created_at = data.get("created_at")
        task.started_at = data.get("started_at")
        task.completed_at = data.get("completed_at")
        task.status = data.get("status")
        task.result = data.get("result")
        task.error = data.get("error")
        return task