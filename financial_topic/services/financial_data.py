import random
import uuid
from datetime import datetime
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
    
    def get_account_and_category(self):
        account_list = [
            'X-100',
            'X-102',
            'X-200',
            'X-202',
            'X-300',
            'X-302'
        ]

        dict_account_category = {
            'X-100': 'BASIC',
            'X-102': 'VIOLET',
            'X-200': 'VIOLET',
            'X-202': 'BLACK',
            'X-300': 'BLACK',
            'X-302': 'VIOLET'
        }
        sorted_account = random.choice(account_list)
        category_name = dict_account_category.get(sorted_account)
        
        return sorted_account, category_name


def generate_transaction_bank() -> dict:
        fake = Faker(['pt_BR'])
        fake.add_provider(CustomProvider)
       #generate_transaction_bank(account_id: str, card_category: Literal['BASIC', 'BLACK', 'VIOLET'])
        sorted_account, category_name = fake.get_account_and_category()

        return {
            'transaction_id': str(uuid.uuid4()),
            'account_id': sorted_account,
            'type': fake.transaction_type(),
            'card_category': category_name,
            'card_number': fake.credit_card_number(),
            'is_approved': fake.return_boolean(),
            'datetime': datetime.now().isoformat()
        }
