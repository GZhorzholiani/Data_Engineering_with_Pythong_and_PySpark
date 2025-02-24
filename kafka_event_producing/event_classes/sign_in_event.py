import datetime


class SignInEvent:
    def __init__(self):
        pass

    def __call__(self, user_id):
        return {
            "timestamp": datetime.datetime.now().isoformat(),
            "event_name": "sign_in",
            "user_id": user_id,
        }

    @staticmethod
    def to_dict(self: dict, ctx) -> dict:
        return {
            "timestamp": self.get("timestamp"),
            "event_name": self.get("event_name"),
            "user_id": self.get("user_id"),
        }
