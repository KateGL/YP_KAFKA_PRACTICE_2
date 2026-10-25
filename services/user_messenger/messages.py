import json
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional

@dataclass
class MessagePayload:
    user_id: str
    timestamp: int

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(data: str) -> "MessagePayload":
        try:
            obj = json.loads(data)
            return MessagePayload(**obj)
        except Exception as e:
            raise ValueError(f"Ошибка десериализации JSON: {e}")


@dataclass
class UserRegisteredMessage(MessagePayload):
    event: str


@dataclass
class UserBlockMessage(MessagePayload):
    event: str
    target_user_id: str

@dataclass
class WordBlockMessage(MessagePayload):
    event: str
    word:str