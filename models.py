from sqlalchemy import Column, String, DateTime, JSON
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()


class Task(Base):
    __tablename__ = "tasks"

    task_id       = Column(String, primary_key=True, index=True)
    status        = Column(String, default="pending", index=True)
    original_path = Column(String, nullable=False)
    sizes         = Column(JSON, nullable=True)
    error         = Column(String, nullable=True)
    created_at    = Column(DateTime, default=datetime.utcnow)
    updated_at    = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)