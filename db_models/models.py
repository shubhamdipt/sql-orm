from sql_orm.postgresql.tables import PostgreSQLTable
from sql_orm.postgresql import datatypes


class Currency(PostgreSQLTable):

    _schema = "personal"

    id = datatypes.DefaultPrimaryKeyField(verbose_name="ID")
    code = datatypes.CharField(max_length=3, verbose_name="Code")


class Bank(PostgreSQLTable):

    _schema = "personal"

    id = datatypes.DefaultPrimaryKeyField(verbose_name="ID")
    name = datatypes.CharField(max_length=50)
    currency = datatypes.ForeignKeyField(
        table_name=Currency,
        verbose_name="Currency",
        null=True
    )


class Transactions(PostgreSQLTable):

    id = datatypes.DefaultPrimaryKeyField(verbose_name="ID")
    date_of_entry = datatypes.DateField(verbose_name="Date of entry")
    datetime_of_entry = datatypes.DateTimeField(verbose_name="Date and Time of entry", null=True)
    amount = datatypes.FloatField(verbose_name="Amount")
    status = datatypes.BooleanField(verbose_name="Status", default=False)
    bank = datatypes.ForeignKeyField(
        table_name=Bank,
        verbose_name="Bank",
        null=True
    )

    def transaction_method(self):
        return "Ok"


class InterBankStatus(PostgreSQLTable):

    id = datatypes.DefaultPrimaryKeyField(verbose_name="ID")
    status = datatypes.BooleanField(verbose_name="Status", default=True)
    depositor = datatypes.ForeignKeyField(
        table_name=Bank,
        verbose_name="Bank",
        null=True
    )
    receiver = datatypes.ForeignKeyField(
        table_name=Bank,
        verbose_name="Bank",
        null=True
    )


class InterBankTransaction(PostgreSQLTable):

    id = datatypes.DefaultPrimaryKeyField(verbose_name="ID")
    amount = datatypes.IntegerField(verbose_name="Amount")
    banks_involved = datatypes.ForeignKeyField(
        table_name=InterBankStatus,
        verbose_name="Inter bank trans",
    )
