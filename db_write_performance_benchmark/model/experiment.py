from typing import Optional

from pydantic import BaseModel


class Experiment(BaseModel):
    run_id: str
    framework: str
    database: str
    start_time: float
    end_time: float
    swap_usage: float
    n_rows: Optional[int]
