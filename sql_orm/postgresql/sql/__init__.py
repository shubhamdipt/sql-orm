class InvalidQueryException(Exception):
    pass


def create_table():
    return "CREATE TABLE IF NOT EXISTS public.{name} ({add_primary_key});"


def add_table_column():
    return "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {name} {field_type} {properties};"


def insert_table_row():
    return "INSERT INTO public.{table_name} ({column_names}) VALUES ({column_values}) RETURNING id;"


def update_table_row():
    return "UPDATE public.{table_name} SET {set_key_value} WHERE {condition};"


class Query:

    def __init__(self, table_name, table_columns, pk, order_dict=None, filter_dict=None, exclude_dict=None, limit=None, offset=None, delete=False):
        self.table_name = table_name
        self.table_columns = table_columns
        self.pk = pk
        self.order_dict = order_dict if order_dict else {"id": "ASC"}
        self.filter_dict = filter_dict
        self.exclude_dict = exclude_dict
        self.limit = limit
        self.offset = offset
        self.delete = delete
        self.operators = {
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
            "isnull": "IS"
        }
        self.base_query = "SELECT {};" if not delete else "DELETE {};"

    def __change_to_sql_conditions(self, key, value):
        if "__" not in key:
            return "{key}={value}".format(key=key, value=value)
        key, condition = key.rsplit("__", 1)
        if key == "pk":
            key = self.pk
        if condition != "isnull":
            return "{key}{operation}{value}".format(
                key=key,
                operation=self.operators[condition],
                value=value
            )
        else:
            base = "{key} {operation}".format(
                key=key,
                operation=self.operators[condition]
            )
            return "{} NULL".format(base) if value else "{} NOT NULL".format(base)

    def __create_where_query(self):
        filter_query = ""
        if self.filter_dict:
            filter_query += " AND ".join([
                self.__change_to_sql_conditions(k, v)
                for k, v in self.filter_dict.items()])
        if self.exclude_dict:
            if self.filter_dict:
                filter_query += " AND "
            filter_query += "NOT "
            filter_query += " AND NOT ".join([
                self.__change_to_sql_conditions(k, v)
                for k, v in self.exclude_dict.items()])
        if filter_query:
            filter_query = " WHERE {}".format(filter_query)
        return filter_query

    def __create_from_query(self):
        if self.delete:
            from_query = "FROM {}".format(self.table_name)
        else:
            from_query = "{} FROM {}".format(", ".join(self.table_columns), self.table_name)
        return from_query

    def __create_order_by_query(self):
        order_query = ""
        if not self.delete:
            if self.order_dict:
                order_query = ", ".join(
                    ["{} {}".format(k, v) for k, v in self.order_dict.items()])
            if order_query:
                order_query = " ORDER BY {}".format(order_query)
        return order_query

    def __create_limit_offset_query(self):
        query = ""
        if self.limit is not None:
            query += " LIMIT {}".format(self.limit)
        if self.offset:
            query += " OFFSET {}".format(self.offset)
        return query

    def query(self):
        query = (
            self.__create_from_query() +
            self.__create_where_query() +
            self.__create_order_by_query() +
            self.__create_limit_offset_query()
        )
        return self.base_query.format(query)
