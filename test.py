from db_models.models import *
from migrate import run_migrations
from datetime import datetime, timedelta
import unittest


def create_objects():
    InterBankTransaction.objects.delete()
    InterBankStatus.objects.delete()
    Transactions.objects.delete()
    Bank.objects.delete()
    Currency.objects.delete()

    currency_1, _ = Currency.objects.get_or_create(code="USD")
    currency_2, _ = Currency.objects.get_or_create(code="EUR")
    currency_3, _ = Currency.objects.get_or_create(code="INR")

    bank_1, _ = Bank.objects.get_or_create(
        name="First Bank name",
        currency=currency_1.id
    )
    bank_2, _ = Bank.objects.get_or_create(
        name="Second Bank name",
        currency=currency_2.id
    )
    bank_3, _ = Bank.objects.get_or_create(
        name="Third Bank name",
        currency=currency_3.id
    )
    transaction_1 = Transactions.objects.create(
        date_of_entry=datetime.now().date(),
        amount=-1,
        status=True,
        bank=bank_1.id
    )
    transaction_2 = Transactions.objects.create(
        date_of_entry=(datetime.now() - timedelta(days=5)).date(),
        datetime_of_entry=datetime.now(),
        amount=-2,
        status=False,
        bank=bank_1.id
    )
    Transactions.objects.bulk_create([
        {
            "date_of_entry": (datetime.now() - timedelta(days=5)).date(),
            "datetime_of_entry": datetime.now(),
            "amount": 1,
            "status": False,
            "bank": bank_2.id
        },
        {
            "date_of_entry": (datetime.now() - timedelta(days=5)).date(),
            "datetime_of_entry": datetime.now(),
            "amount": 2,
            "status": False,
            "bank": bank_2.id
        },
        {
            "date_of_entry": (datetime.now() - timedelta(days=5)).date(),
            "datetime_of_entry": datetime.now(),
            "amount": 3,
            "status": False,
            "bank": bank_3.id
        }
    ])
    inter_trans_1 = InterBankStatus.objects.create(
        status=True,
        depositor=bank_1.id,
        receiver=bank_2.id
    )
    inter_trans_2 = InterBankStatus.objects.create(
        status=True,
        depositor=bank_2.id,
        receiver=bank_3.id
    )
    InterBankTransaction.objects.bulk_create([
        {
            "amount": 100,
            "banks_involved": inter_trans_1.id
        },
        {
            "amount": 200,
            "banks_involved": inter_trans_1.id
        },
        {
            "amount": 300,
            "banks_involved": inter_trans_2.id
        }
    ])




def update_objects():
    bank, _ = Bank.objects.get_or_create(
        name="Bank name",
        currency__code="USD"
    )
    bank.name = "My bank"
    bank.save()
    print([i.name for i in Bank.objects.all()])
    print(Bank.objects.get(name="My bank").name)


class TestORMMethods(unittest.TestCase):

    def setUp(self):
        pass

    def test_object_creation(self):
        obj = Currency.objects.create(code="GBP")
        self.assertEqual(obj.code, "GBP")
        obj, created = Currency.objects.get_or_create(code="JPN")
        self.assertEqual(obj.code, "JPN")
        self.assertEqual(created, True)
        obj, created = Currency.objects.get_or_create(code="JPN")
        self.assertEqual(obj.code, "JPN")
        self.assertEqual(created, False)

    def test_bulk_object_creation(self):
        Currency.objects.bulk_create([
            {
                "code": "BGN"
            },
            {
                "code": "NOK"
            }
        ])
        obj, created = Currency.objects.get_or_create(code="BGN")
        self.assertEqual(obj.code, "BGN")
        self.assertEqual(created, False)

    def test_query_deletion(self):
        Currency.objects.filter(code__in=["BGN", "NOK"]).delete()
        currencies = sorted([i.code for i in Currency.objects.all()])
        self.assertEqual(currencies, ['EUR', 'GBP', 'INR', 'JPN', 'USD'])

    def test_query_count(self):
        self.assertEqual(Bank.objects.count(), 3)

    def test_query_order_by(self):
        all_currencies_1 = [i.code for i in Currency.objects.order_by('code')]
        all_currencies_2 = [i.code for i in Currency.objects.order_by('-code')]
        self.assertEqual(all_currencies_1, ['EUR', 'GBP', 'INR', 'JPN', 'USD'])
        self.assertEqual(all_currencies_2, ['USD', 'JPN', 'INR', 'GBP', 'EUR'])

    def test_query_indexing(self):
        currency_1 = Currency.objects.order_by('-code')[2]
        currency_2 = Currency.objects.order_by('-code')[3]
        self.assertEqual(currency_1.code, "INR")
        self.assertEqual(currency_2.code, "GBP")

    def test_query_slicing(self):
        all_currencies_1 = [i.code for i in Currency.objects.order_by('-code')[:2]]
        all_currencies_2 = [i.code for i in Currency.objects.order_by('-code')[2:4]]
        self.assertEqual(all_currencies_1, ['USD', 'JPN'])
        self.assertEqual(all_currencies_2, ['INR', 'GBP'])

    def test_query_using_get(self):
        obj_1 = Currency.objects.get(code="INR")
        obj_2 = Currency.objects.get(code="EUR")
        self.assertEqual(obj_1.code, "INR")
        self.assertEqual(obj_2.code, "EUR")

    def test_query_using_filters(self):
        currencies_1 = [i for i in Currency.objects.filter(code__istartswith="I")]
        currencies_2 = [i for i in Currency.objects.filter(code__endswith="D")]
        currencies_3 = sorted([i.code for i in Currency.objects.or_filter(
            code__endswith="R", code__startswith="U")])
        currencies_4 = sorted([i.code for i in Currency.objects.exclude(code__endswith="R")])
        currencies_5 = sorted([i.code for i in Currency.objects.filter(code__in=["GBP", "USD"])])

        self.assertEqual(currencies_1[0].code, "INR")
        self.assertEqual(currencies_2[0].code, "USD")
        self.assertEqual(currencies_3, ["EUR", "INR", "USD"])
        self.assertEqual(currencies_4, ['GBP', 'JPN', 'USD'])
        self.assertEqual(currencies_5, ['GBP', 'USD'])

    def test_query_filters_with_join(self):
        bank_1 = Bank.objects.filter(currency__code="USD")[0]
        bank_2 = Bank.objects.filter(currency__code="INR")[0]
        self.assertEqual(bank_1.name, "First Bank name")
        self.assertEqual(bank_1.currency.code, "USD")
        self.assertEqual(bank_2.name, "Third Bank name")
        self.assertEqual(bank_2.currency.code, "INR")

        trans_1 = Transactions.objects.filter(bank__currency__code="EUR")[0]
        trans_2 = Transactions.objects.filter(bank__currency__code="INR")[0]
        self.assertEqual(trans_1.bank.currency.code, "EUR")
        self.assertEqual(trans_1.bank.name, "Second Bank name")
        self.assertEqual(trans_1.amount, 1)
        self.assertEqual(trans_2.bank.currency.code, "INR")
        self.assertEqual(trans_2.bank.name, "Third Bank name")
        self.assertEqual(trans_2.amount, 3)

    def test_query_filters_with_multiple_foreign_keys_to_same_table(self):
        inter_bank_1 = InterBankStatus.objects.get(
            depositor__currency__code="USD"
        )
        inter_bank_2 = InterBankStatus.objects.get(
            receiver__currency__code="INR"
        )
        inter_bank_trans_1 = InterBankTransaction.objects.get(
            banks_involved__depositor__currency__code="EUR",
            banks_involved__receiver__currency__code="INR"
        )
        inter_bank_trans_2 = InterBankTransaction.objects.get(
            banks_involved__depositor__currency__code="EUR",
            amount__gte=300
        )
        inter_bank_trans_3 = InterBankTransaction.objects.filter(
            banks_involved__depositor__currency__code__isnull=False,
            amount__lte=300
        ).order_by("amount")
        self.assertEqual(inter_bank_1.depositor.currency.code, "USD")
        self.assertEqual(inter_bank_1.depositor.name, "First Bank name")
        self.assertEqual(inter_bank_1.receiver.name, "Second Bank name")
        self.assertEqual(inter_bank_1.receiver.currency.code, "EUR")
        self.assertEqual(inter_bank_2.depositor.currency.code, "EUR")
        self.assertEqual(inter_bank_2.depositor.name, "Second Bank name")
        self.assertEqual(inter_bank_2.receiver.name, "Third Bank name")
        self.assertEqual(inter_bank_2.receiver.currency.code, "INR")
        self.assertEqual(inter_bank_trans_1.banks_involved.depositor.currency.code, "EUR")
        self.assertEqual(inter_bank_trans_1.banks_involved.receiver.currency.code, "INR")
        self.assertEqual(inter_bank_trans_1.banks_involved.depositor.name, "Second Bank name")
        self.assertEqual(inter_bank_trans_1.banks_involved.receiver.name, "Third Bank name")
        self.assertEqual(inter_bank_trans_1.amount, 300)
        self.assertEqual(inter_bank_trans_2.banks_involved.depositor.currency.code, "EUR")
        self.assertEqual(inter_bank_trans_2.banks_involved.receiver.currency.code, "INR")
        self.assertEqual(inter_bank_trans_2.banks_involved.depositor.name, "Second Bank name")
        self.assertEqual(inter_bank_trans_2.banks_involved.receiver.name, "Third Bank name")
        self.assertEqual(inter_bank_trans_2.amount, 300)
        self.assertEqual([i.amount for i in inter_bank_trans_3], [100, 200, 300])

    def test_query_select_related(self):
        trans_1 = Transactions.objects.all().select_related().order_by("amount")
        trans_2 = Transactions.objects.filter(amount__lt=3).select_related("bank__currency").order_by("amount")
        inter_bank_1 = InterBankStatus.objects.filter(status=True).order_by("id").select_related()

        self.assertEqual(
            [i.bank.name for i in trans_1],
            ['First Bank name', 'First Bank name', 'Second Bank name', 'Second Bank name', 'Third Bank name']
        )
        self.assertEqual(
            [i.bank.name for i in trans_2],
            ['First Bank name', 'First Bank name', 'Second Bank name', 'Second Bank name']
        )
        self.assertEqual(
            [(i.depositor.name, i.receiver.name) for i in inter_bank_1],
            [('First Bank name', 'Second Bank name'), ('Second Bank name', 'Third Bank name')]
        )

    def test_update_object(self):
        obj, _ = Currency.objects.get_or_create(code="UPD")
        obj.code = "TES"
        obj.save()
        obj_1 = Currency.objects.filter(code="TES")
        self.assertEqual(obj_1.count(), 1)
        self.assertEqual(obj_1[0].code, "TES")
        Currency.objects.filter(code="TES").delete()
        obj_2 = Currency.objects.filter(code="TES")
        self.assertEqual(obj_2.count(), 0)


if __name__ == '__main__':
    run_migrations()
    create_objects()
    unittest.main()
