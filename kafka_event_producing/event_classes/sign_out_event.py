class SignOutEvent:
    def __init__(self):
        self.event_name = "sign_out"

    def __call__(self, user_id, sign_in_timestamp):
        sign_out_info = {
            "timestamp": sign_in_timestamp.isoformat(),
            "event_name": self.event_name,
            "user_id": user_id
        }
        return sign_out_info

    @staticmethod
    def to_dict(self: dict, ctx) -> dict:
        return {
            "timestamp": self.get("timestamp"),
            "event_name": self.get("event_name"),
            "user_id": self.get("user_id")
        }