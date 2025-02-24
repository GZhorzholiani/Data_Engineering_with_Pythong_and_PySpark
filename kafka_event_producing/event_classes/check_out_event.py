import datetime
import random
from typing import Dict, Union


class CheckOutEvent:

    def __init__(self):
        pass

    def get_payment_method(self) -> str:
        payment_methods = ["Cash", "Card"]

        return random.choice(payment_methods)

    def __call__(self, user_id, cart_id) -> Dict[str, Union[str, int]]:
        return {
            'timestamp': datetime.datetime.now().isoformat(),
            'event_name': 'checkout_to_cart',
            'user_id': user_id,
            'cart_id': cart_id,
            'payment_method': self.get_payment_method()
        }

    @staticmethod
    def to_dict(check_out_event_object: dict, ctx) -> Dict[str, Union[str, int]]:
        return {
            'timestamp': check_out_event_object.get('timestamp'),
            'event_name': check_out_event_object.get('event_name'),
            'user_id': check_out_event_object.get('user_id'),
            'cart_id': check_out_event_object.get('cart_id'),
            'payment_method': check_out_event_object.get('payment_method'),
        }
