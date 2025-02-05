# scraper/tasks.py
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

@dataclass
class ScrapingTask:
    url: str
    company_id: str
    priority: int = 1
    retry_count: int = 0
    last_attempt: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        return {
            "url": self.url,
            "company_id": self.company_id,
            "priority": self.priority,
            "retry_count": self.retry_count,
            "last_attempt": self.last_attempt.isoformat() if self.last_attempt else None
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'ScrapingTask':
        if data.get('last_attempt'):
            data['last_attempt'] = datetime.fromisoformat(data['last_attempt'])
        return cls(**data)