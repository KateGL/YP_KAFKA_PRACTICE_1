import json
from dataclasses import dataclass, asdict
from typing import Any, Optional

@dataclass
class Message:
    id: str
    payload: Any
    metadata: Optional[dict] = None

    def serialize(self) -> bytes:
        try:
            data = asdict(self)
            return json.dumps(data, ensure_ascii=False).encode("utf-8")
        except Exception as e:
            raise ValueError(f"Serialization error: {e}")

    @staticmethod
    def deserialize(data: bytes) -> "Message":
        try:
            obj = json.loads(data.decode("utf-8"))
            return Message(**obj)
        except Exception as e:
            raise ValueError(f"Deserialization error: {e}")
