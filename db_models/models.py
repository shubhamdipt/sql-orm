from sql_orm.postgresql.tables import PostgreSQLTable
from sql_orm.postgresql import datatypes


class Transactions(PostgreSQLTable):

    id = datatypes.DefaultPrimaryKeyField(name="id")
    date_of_entry = datatypes.DateField(name="date_of_entry")
    amount = datatypes.FloatField(name="amount")
    currency = datatypes.CharField(name="currency", max_length=10)
