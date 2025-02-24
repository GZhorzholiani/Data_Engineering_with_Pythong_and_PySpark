import datetime
import random

class ItemViewEvent:
    def __init__(self):
        pass

    def get_item_id(self) -> str:
        random_item_id = str(random.randint(1, 8701))
        return random_item_id

    def __call__(self, user_id):
        item_view_info = {
            "timestamp": datetime.datetime.now().isoformat(),
            "event_name": "item_view",
            "user_id": user_id,
            "item_id": self.get_item_id()
        }
        return item_view_info

    @staticmethod
    def to_dict(self: dict, ctx) -> dict:
        return {
            "timestamp": self.get("timestamp"),
            "event_name": self.get("event_name"),
            "user_id": self.get("user_id"),
            "item_id": self.get("item_id")
        }