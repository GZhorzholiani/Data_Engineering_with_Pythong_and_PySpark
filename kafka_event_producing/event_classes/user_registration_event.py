import datetime
import random


class UserRegistrationEvent:
    def __init__(self):
        self.user_ids_file = "../user_ids.txt"
        try:
            with open(self.user_ids_file, "r") as file:
                self.existing_user_ids = set(file.read().splitlines())
        except FileNotFoundError:
            self.existing_user_ids = set()

    def user_id(self) -> int:
        while True:
            random_unique_number = random.randint(1, 1000000)
            if str(random_unique_number) not in self.existing_user_ids:
                self.existing_user_ids.add(str(random_unique_number))
                with open(self.user_ids_file, "a") as file:
                    file.write(f"{random_unique_number}\n")
                return random_unique_number

    def age(self) -> int:
        return random.randint(18, 95)

    def masked_email(self) -> str:
        return random.choice(["****@gmail.com", "****@example.com", "****@yahoo.com", "****@outlook.com"])

    def preferred_language(self) -> str:
        return random.choice(["eng", "geo", " "])

    def __call__(self):
        return {
            "timestamp": datetime.datetime.now().isoformat(),
            "event_name": "consumer_registration",
            "user_id": self.user_id(),
            "age": self.age(),
            "masked_email": self.masked_email(),
            "preferred_language": self.preferred_language(),
        }

    @staticmethod
    def to_dict(self: dict, ctx) -> dict:
        return {
            "timestamp": self.get("timestamp"),
            "event_name": self.get("event_name"),
            "user_id": self.get("user_id"),
            "age": self.get("age"),
            "masked_email": self.get("masked_email"),
            "preferred_language": self.get("preferred_language"),
        }
