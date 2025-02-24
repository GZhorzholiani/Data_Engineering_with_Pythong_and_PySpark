import datetime
import random
import uuid
from typing import Dict, Union


class AddedToCartEvent:

    def __init__(self):
        pass

    def get_item_id(self) -> str:
        """
        generate random id from existing items
        :return:
        """
        return str(random.randint(1, 8701))

    def get_cart_id(self):
        random_uuid = uuid.uuid4()
        return str(random_uuid)

    def __call__(self, user_id) -> Dict[str, Union[str, int]]:
        return {
            'timestamp': datetime.datetime.now().isoformat(),
            'event_name': 'added_to_cart',
            'item_id': self.get_item_id(),
            'user_id': user_id,
            'cart_id': self.get_cart_id()
        }

    @staticmethod
    def to_dict(added_to_cart_event_object: dict, ctx) -> Dict[str, Union[str, int]]:
        return {
            'timestamp': added_to_cart_event_object.get('timestamp'),
            'event_name': added_to_cart_event_object.get('event_name'),
            'item_id': added_to_cart_event_object.get('item_id'),
            'user_id': added_to_cart_event_object.get('user_id'),
            'cart_id': added_to_cart_event_object.get('cart_id')
        }
