import json
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional

@dataclass
class MessagePayload:
    id: int
    source: str
    payload: Any
    metadata: Optional[Dict[str, Any]] = None

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(data: str) -> "MessagePayload":
        try:
            obj = json.loads(data)
            return MessagePayload(**obj)
        except Exception as e:
            raise ValueError(f"Ошибка десериализации JSON: {e}")
