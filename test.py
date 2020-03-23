from db_models.models import *
from migrate import run_migrations
from datetime import datetime, timedelta


def create_objects():
    Transactions.objects.delete()
    Bank.objects.delete()
    Currency.objects.delete()

    currency, _ = Currency.objects.get_or_create(code="USD")
    bank, _ = Bank.objects.get_or_create(
        name="Bank name",
        currency=currency.id
    )
    transaction_1= Transactions.objects.create(
        date_of_entry=datetime.now().date(),
        amount=12.55,
        status=True,
        bank=bank.id
    )
    transaction_2 = Transactions.objects.create(
        date_of_entry=(datetime.now() - timedelta(days=5)).date(),
        datetime_of_entry=datetime.now(),
        amount=1234,
        status=False,
        bank=bank.id
    )


def query_objects():
    print("Currencies")
    print(Currency.objects.get(code="USD").as_dict())
    print(Currency.objects.filter(code="USD")[0].as_dict())

    print("Banks")
    banks = Bank.objects.all()
    print([{"name": i.name} for i in banks])
    banks = Bank.objects.all().select_related()
    print([{"name": i.name, "currency": i.currency.code} for i in banks])

    print("Transactions")
    trans = Transactions.objects.all()
    print([{"date_of_entry": i.date_of_entry,
            "amount": i.amount,
            "status": i.status} for i in trans])

    trans = Transactions.objects.filter(amount__gt=100)
    for obj in trans:
        print("Amount greater than 100", obj.id, obj.amount)

    trans = Transactions.objects.filter(bank__currency__code__startswith="USD")
    for obj in trans:
        print("Currency USD", obj.id, obj.bank.currency.code, obj.datetime_of_entry)


def update_objects():
    bank, _ = Bank.objects.get_or_create(
        name="Bank name",
        currency__code="USD"
    )
    bank.name = "My bank"
    bank.save()
    print(Bank.objects.get(name="My bank").name)


def main():
    run_migrations()
    create_objects()
    query_objects()
    update_objects()


if __name__ == '__main__':
    main()