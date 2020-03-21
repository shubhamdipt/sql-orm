from sql_orm import DATABASE_TYPES, BaseField
from sql_orm.postgresql import sql


class Field(BaseField):

    database_type = DATABASE_TYPES["PostgreSQL"]
    field_type = None
    __value = None

    def __init__(self, verbose_name=None, null=False, unique=False, primary_key=False, default=None, extra_sql=()):
        self.verbose_name = verbose_name
        self.primary_key = primary_key
        if primary_key:
            self.properties = "PRIMARY KEY"
        else:
            if default is not None:
                self.properties = "DEFAULT {}".format("TRUE" if default else "FALSE")
            else:
                self.properties = "NULL" if null else "NOT NULL"
                if unique:
                    self.properties = "UNIQUE " + self.properties
        if extra_sql:
            self.properties += " " + " ".join(extra_sql)
        self.base_create_query = sql.add_table_column()

    @staticmethod
    def convert(value):
        return value

    def __set__(self, instance, value):
        self.__value = self.convert(value) if value is not None else None

    def __get__(self, instance, owner):
        return self.__value

    def create(self, schema, table_name, column_name):
        query = self.base_create_query.format(
            schema=schema,
            table_name=table_name,
            name=column_name,
            field_type=self.field_type,
            properties=self.properties
        )
        return query


class BooleanField(Field):

    field_type = "BOOLEAN"

    @staticmethod
    def convert(value):
        return bool(value)


class IntegerField(Field):

    field_type = "INTEGER"

    @staticmethod
    def convert(value):
        return int(value)


class FloatField(Field):

    field_type = "NUMERIC(15,6)"

    @staticmethod
    def convert(value):
        return float(value)


class DefaultPrimaryKeyField(IntegerField):

    field_type = "SERIAL"

    def __init__(self, verbose_name=None):
        super().__init__(verbose_name=verbose_name, primary_key=True)

    @staticmethod
    def convert(value):
        return int(value)


class DateField(Field):

    field_type = "DATE"


class DateTimeField(Field):

    field_type = "TIMESTAMPTZ"


class CharField(Field):

    field_type = "VARCHAR"

    def __init__(self, max_length, verbose_name=None, null=False, unique=False):
        super().__init__(verbose_name=verbose_name, null=null, unique=unique)
        self.properties = "({}) {}".format(max_length, self.properties)

    @staticmethod
    def convert(value):
        return str(value)


class ForeignKeyField(Field):

    def __init__(self, table_name, verbose_name=None, null=False, unique=False):
        pk = table_name.get_pk_name()
        self.table_name = table_name
        self.field = table_name.__dict__[pk]
        extra_sql = "REFERENCES {schema}.{table_name}({pk})".format(
            schema=table_name.get_schema(),
            table_name=table_name.get_table_name(),
            pk=pk
        )
        self.field_type = "INTEGER" if self.field.field_type == "SERIAL" else self.field.field_type
        super().__init__(verbose_name=verbose_name, null=null, unique=unique, extra_sql=(extra_sql, ))

    def __set__(self, instance, value):
        self.__value = value

    def __get__(self, instance, owner):
        if self.__value is not None:
            if isinstance(self.__value, self.table_name):
                return self.__value
            value = self.table_name.get_value_or_object_pk(self.__value)
            value = self.table_name.objects.get(pk=self.field.convert(value))
            return value
        return None
