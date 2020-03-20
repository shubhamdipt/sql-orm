# from sql_orm.postgresql.tables import PostgreSQLTable
# from sql_orm.postgresql import datatypes
#
#
# class Currency(PostgreSQLTable):
#
#     _schema = "personal"
#
#     id = datatypes.DefaultPrimaryKeyField(verbose_name="ID")
#     code = datatypes.CharField(max_length=3, verbose_name="Code")
#
#
# class Bank(PostgreSQLTable):
#
#     _schema = "personal"
#
#     id = datatypes.DefaultPrimaryKeyField(verbose_name="ID")
#     name = datatypes.CharField(max_length=50)
#     currency = datatypes.ForeignKeyField(
#         table_name=Currency,
#         verbose_name="Currency",
#         null=True
#     )
#
#
# class Transactions(PostgreSQLTable):
#
#     id = datatypes.DefaultPrimaryKeyField(verbose_name="ID")
#     date_of_entry = datatypes.DateField(verbose_name="Date of entry")
#     amount = datatypes.FloatField(verbose_name="Amount", default=0)
#     status = datatypes.BooleanField(verbose_name="Status", default=False)
#     bank = datatypes.ForeignKeyField(
#         table_name=Bank,
#         verbose_name="Bank",
#         null=True
#     )
#
#     def transaction_method(self):
#         return "Ok"



from sql_orm.sqlite.tables import SQLiteTable
from sql_orm.sqlite import datatypes


class Currency(SQLiteTable):

    _schema = "personal"

    id = datatypes.DefaultPrimaryKeyField(verbose_name="ID")
    code = datatypes.CharField(verbose_name="Code", default="")


class Bank(SQLiteTable):

    _schema = "personal"

    id = datatypes.DefaultPrimaryKeyField(verbose_name="ID")
    name = datatypes.CharField(verbose_name="Name", default="")
    currency = datatypes.ForeignKeyField(
        table_name=Currency,
        verbose_name="Currency",
        null=True
    )


class Transactions(SQLiteTable):

    id = datatypes.DefaultPrimaryKeyField(verbose_name="ID")
    date_of_entry = datatypes.DateField(verbose_name="Date of entry", default="")
    amount = datatypes.FloatField(verbose_name="Amount", default=0)
    status = datatypes.BooleanField(verbose_name="Status", default=False)
    bank = datatypes.ForeignKeyField(
        table_name=Bank,
        verbose_name="Bank",
        null=True
    )

    def transaction_method(self):
        return "Ok"
