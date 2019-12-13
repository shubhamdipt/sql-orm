from sql_orm.postgresql.tables import PostgreSQLTable
from sql_orm.postgresql import datatypes


class Bank(PostgreSQLTable):

    _schema = "personal"

    id = datatypes.DefaultPrimaryKeyField(verbose_name="ID")
    name = datatypes.CharField(max_length=50)


class Transactions(PostgreSQLTable):

    id = datatypes.DefaultPrimaryKeyField(verbose_name="ID")
    date_of_entry = datatypes.DateField(verbose_name="Date of entry")
    amount = datatypes.FloatField(verbose_name="Amount")
    currency = datatypes.CharField(max_length=10)
    bank = datatypes.ForeignKeyField(
        table_name=Bank,
        verbose_name="Bank",
        null=True
    )
