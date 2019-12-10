from sql_orm import DATABASE_TYPES
from sql_orm.postgresql import sql
from datetime import datetime


class Field:

    database_type = DATABASE_TYPES["PostgreSQL"]
    field_type = None
    __value = None

    def __init__(self, name, null=False, unique=False, primary_key=False, extra_sql=()):
        self.name = name
        self.primary_key = primary_key
        if primary_key:
            self.properties = "PRIMARY KEY"
        else:
            self.properties = "NULL" if null else "NOT NULL"
            if unique:
                self.properties = "UNIQUE " + self.properties
        if extra_sql:
            self.properties += " " + " ".join(extra_sql)
        self.base_create_query = sql.add_table_column()

    @staticmethod
    def _convert(value):
        return value

    def __set__(self, instance, value):
        self.__value = self._convert(value) if value is not None else None

    def __get__(self, instance, owner):
        return self.__value

    def create(self, table_name):
        query = self.base_create_query.format(
            table_name=table_name,
            name=self.name,
            field_type=self.field_type,
            properties=self.properties
        )
        return query


class IntegerField(Field):

    field_type = "INTEGER"

    @staticmethod
    def _convert(value):
        return int(value)


class FloatField(Field):

    field_type = "NUMERIC(15,6)"

    @staticmethod
    def _convert(value):
        return float(value)


class DefaultPrimaryKeyField(IntegerField):

    field_type = "SERIAL"

    def __init__(self, name):
        super().__init__(name=name, primary_key=True)

    @staticmethod
    def _convert(value):
        return int(value)


class DateField(Field):

    field_type = "DATE"


class CharField(Field):

    field_type = "VARCHAR"

    def __init__(self, name, max_length, null=False, unique=False):
        super().__init__(name=name, null=null, unique=unique)
        self.properties = "({}) {}".format(max_length, self.properties)

    @staticmethod
    def _convert(value):
        return str(value)
