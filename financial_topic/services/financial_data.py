import random
import uuid
from datetime import datetime
from typing import Literal
import logging
import faker
from faker.providers import BaseProvider
from faker import Faker


class CustomProvider(BaseProvider):
    import random

    def transaction_type(self):
        transaction_types = ['CREDIT_PURCHASE', 'DEBIT_PURCHASE', 'WITHDRAW']
        return self.random.choice(transaction_types)
    
    def card_category(self):
        category_types = ['BASIC', 'BLACK', 'VIOLET']
        return self.random.choice(category_types)
    
    def return_boolean(self):
        approved_list = ['TRUE', 'FALSE']
        return self.random.choice(approved_list)
    
    def account_id(self):
        account_list = [
            '',
            '',
            '',
            '',
            ''
        ]
        pass


if __name__ == '__main__':
    log_format = "[%(asctime)s] - [%(name)s] - [%(levelname)s] - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format, datefmt="%Y-%m-%d %H:%M:%S")

    fake = Faker(['pt_BR'])
    fake.add_provider(CustomProvider)


    def generate_transaction_bank(account_id: str, card_category: Literal['BASIC', 'BLACK', 'VIOLET']):
        return {
            'id': uuid.uuid4(),
            'type': fake.transaction_type(),
            'card_category': card_category,
            'card_number': fake.credit_card_number(),
            'is_approved': fake.return_boolean(),
            'account_id': account_id,
            'datetime': datetime.now().isoformat()
        }

    logging.info("STARTING THE INGESTION FOR `CARD-TRANSACTIONS-TOPIC`")
    account_id = input("account_id:")
    card_category = input("card_category:")
    data = generate_transaction_bank(account_id=account_id, card_category=card_category)
    logging.info(f"data: {data}")

