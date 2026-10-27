import faust
import json


class BaseMessagePayload(faust.Record):
    user_id: str
    timestamp: int

    def to_json(self) -> str:
        return json.dumps(self.asdict())

    @classmethod
    def from_json(cls, data: str):
        try:
            obj = json.loads(data)
            return cls(**obj)
        except Exception as e:
            raise ValueError(f"Ошибка десериализации JSON: {e}")



class UserRegisteredMessage(BaseMessagePayload):
    event: str


class UserBlockMessage(BaseMessagePayload):
    event: str
    target_user_id: str


class WordBlockMessage(BaseMessagePayload):
    event: str
    word:str


class MessageForChatting(BaseMessagePayload):
    target_user_id: str
    text: str