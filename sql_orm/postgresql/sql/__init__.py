from sql_orm import FKFieldTree
from collections import OrderedDict


class InvalidQueryException(Exception):
    pass


def create_schema():
    return "CREATE SCHEMA IF NOT EXISTS {name};"


def create_table():
    return "CREATE TABLE IF NOT EXISTS {schema}.{name} ({add_primary_key});"


def add_table_column():
    return 'ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS "{name}" {field_type} {properties};'


def insert_table_row():
    return "INSERT INTO {schema}.{table_name} ({column_names}) VALUES ({column_values}) RETURNING id;"


def update_table_row():
    return "UPDATE {schema}.{table_name} SET {set_key_value} WHERE {condition};"


def add_unique_together():
    return "ALTER TABLE {schema}.{table_name} ADD CONSTRAINT {constraint_name} UNIQUE ({columns});"


LOGICAL_SEPARATOR = "__"


class Query:

    def __init__(
            self,
            schema,
            table_class,
            table_columns,
            pk,
            order_dict=None,
            filter_dict=None,
            or_filter_dict=None,
            exclude_dict=None,
            select_related=None,
            limit=None,
            offset=None,
            delete=False
    ):
        self.__schema = schema
        self.__table_class = table_class
        self.__table_name = table_class.get_table_name()
        self.__full_table_name = table_class.get_full_table_name()
        self.__table_columns = table_columns
        self.__pk = pk

        self.__order_dict = order_dict if order_dict else {"id": "ASC"}
        self.__filter_dict = filter_dict if filter_dict else {}
        self.__or_filter_dict = or_filter_dict if or_filter_dict else {}
        self.__exclude_dict = exclude_dict if exclude_dict else {}
        self.__select_related = select_related if select_related else []

        self.__limit = limit
        self.__offset = offset
        self.__delete = delete
        self.__operators = {
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
            "exact": "=",
            "iexact": "=",
            "contains": "LIKE",
            "icontains": "ILIKE",
            "startswith": "LIKE",
            "istartswith": "ILIKE",
            "endswith": "LIKE",
            "iendswith": "ILIKE",
            "isnull": "IS",
            "in": "=",
        }
        self.__params = []
        self.__base_query = "SELECT {};" if not delete else "DELETE {};"
        self.__from_query = ""
        self.__where_query = ""
        self.__order_by_query = ""
        self.__limit_offset_query = ""
        self.__column_query = ""
        self.__proxy_name_count = -1
        self.__base_table_proxy = self.generate_table_name_proxy()
        self.__join_tables_involved = {}
        self.__table_details = OrderedDict()

    def generate_table_name_proxy(self):
        self.__proxy_name_count += 1
        return "table_{}".format(self.__proxy_name_count)

    def __update_join_tables_involved(self, base_table_class, fk_table_class, key, avoid_duplicates=False, last_proxy=None):
        last_proxy = last_proxy if last_proxy else self.__base_table_proxy
        join_data = {
            "parent_proxy": last_proxy,
            "details": {
                "fk_table_name": "{}".format(fk_table_class.get_full_table_name()),
                "base_table_name": "{}".format(base_table_class.get_full_table_name()),
                "key": key,
                "fk_table_pk": fk_table_class.get_pk_name(),
                "fk_columns": fk_table_class.get_column_names(),
                "fk_table_class": fk_table_class,
                "base_table_class": base_table_class
            }
        }
        fk_details_key = (join_data["details"]["fk_table_name"], join_data["details"]["base_table_name"], key)
        if avoid_duplicates and fk_details_key in self.__join_tables_involved:
            return self.__join_tables_involved[fk_details_key]
        else:
            proxy = self.generate_table_name_proxy()
            self.__table_details[proxy] = join_data
            self.__join_tables_involved[fk_details_key] = proxy
            return proxy

    def __logical_conditions(self, key, value, condition, table_proxy_name=None):
        if not table_proxy_name:
            table_proxy_name = self.__base_table_proxy
        if key == "pk":
            key = self.__pk
        key = "{}.{}".format(table_proxy_name, key)
        if condition == "=":
            self.__params.append(value)
            return "{key}=%s".format(key=key)
        if condition == "in":
            if isinstance(value, list):
                self.__params.append(value)
                return "{key} {operation} ANY(%s)".format(
                    key=key,
                    operation=self.__operators[condition]
                )
            else:
                raise InvalidQueryException("Value should be a list.")
        if condition == "isnull":
            base = "{key} {operation}".format(
                key=key,
                operation=self.__operators[condition]
            )
            return "{} NULL".format(base) if value else "{} NOT NULL".format(base)
        if condition == "iexact":
            self.__params.append(value)
            return "LOWER({key}){operation}LOWER(%s)".format(
                key=key,
                operation=self.__operators[condition],
            )
        if condition in ("contains", "icontains", "startswith", "istartswith", "endswith", "iendswith"):
            if condition == "contains" or condition == "icontains":
                self.__params.append("%{}%".format(value))
            elif condition == "startswith" or condition == "istartswith":
                self.__params.append("{}%".format(value))
            elif condition == "endswith" or condition == "iendswith":
                self.__params.append("%{}".format(value))
            return "{key} {operation} %s".format(
                key=key,
                operation=self.__operators[condition],
            )
        self.__params.append(value)
        return "{key}{operation}%s".format(
            key=key,
            operation=self.__operators[condition],
        )

    def __change_to_sql_conditions(self, key, value):
        key_splits = key.split(LOGICAL_SEPARATOR)
        if len(key_splits) == 1:
            return self.__logical_conditions(
                key=key_splits[0],
                value=value,
                condition="="
            )
        else:
            proxy = None
            if key_splits[-1] in self.__operators.keys():
                condition = key_splits[-1]
                key = key_splits[-2]
                fk_fields = key_splits[:-2]
                if not fk_fields:
                    return self.__logical_conditions(
                        key=key,
                        value=value,
                        condition=condition
                    )
            else:
                key = key_splits[-1]
                condition = "="
                fk_fields = key_splits[:-1]

            if fk_fields:
                table_class = self.__table_class
                for fk in fk_fields:
                    fk_table_class = getattr(table_class.__dict__[fk], "table_name")
                    proxy = self.__update_join_tables_involved(
                        base_table_class=table_class,
                        fk_table_class=fk_table_class,
                        key=fk,
                        last_proxy=proxy
                    )
                    table_class = fk_table_class

            return self.__logical_conditions(
                key=key,
                value=value,
                condition=condition,
                table_proxy_name=proxy
            )

    def __create_where_query(self):
        filter_query = ""
        if self.__or_filter_dict:
            filter_query = "( {} )".format(" OR ".join([
                self.__change_to_sql_conditions(k, v)
                for k, v in self.__or_filter_dict.items()]))
        if self.__filter_dict:
            if filter_query:
                filter_query += " AND "
            filter_query += " AND ".join([
                self.__change_to_sql_conditions(k, v)
                for k, v in self.__filter_dict.items()])
        if self.__exclude_dict:
            if filter_query:
                filter_query += " AND "
            filter_query += "NOT "
            filter_query += " AND NOT ".join([
                self.__change_to_sql_conditions(k, v)
                for k, v in self.__exclude_dict.items()])
        if filter_query:
            filter_query = " WHERE {}".format(filter_query)
        self.__where_query = filter_query

    def __process_select_related(self):
        for fk_item in set(self.__select_related):
            proxy = None
            parent_class = self.__table_class
            for fk in fk_item.split("__"):
                fk_table_class = getattr(parent_class.__dict__[fk], "table_name")
                proxy = self.__update_join_tables_involved(
                    base_table_class=parent_class,
                    fk_table_class=fk_table_class,
                    key=fk,
                    last_proxy=proxy,
                    avoid_duplicates=True
                )
                parent_class = fk_table_class

    def __switch_to_join_query(self):
        if self.__delete:
            from_query = "FROM {} AS {} USING ".format(
                self.__full_table_name,
                self.__base_table_proxy
            )
            fk_tables = []
            for proxy, table_details in self.__table_details.items():
                fk_tables.append("{} AS {}".format(
                    table_details["details"]["fk_table_name"],
                    proxy
                ))
            self.__from_query = from_query + ", ".join(fk_tables)
        else:
            join_query = ""
            columns = ["{}.{}".format(self.__base_table_proxy, i) for i in self.__table_class.get_column_names()]

            for proxy_name, table_details in self.__table_details.items():
                columns += ["{}.{}".format(proxy_name, j) for j in table_details["details"]["fk_columns"]]

                fk_table = "{} AS {}".format(
                    table_details["details"]["fk_table_name"],
                    proxy_name
                )
                on_join = "{}.{}={}.{}".format(
                    proxy_name,
                    table_details["details"]["fk_table_pk"],
                    table_details["parent_proxy"],
                    table_details["details"]["key"]
                )
                join_query += " LEFT JOIN " + fk_table + " ON " + on_join

            self.__column_query = ", ".join(columns)
            self.__from_query = "{} FROM {} AS {}{}".format(
                self.__column_query,
                self.__full_table_name,
                self.__base_table_proxy,
                join_query
            )

    def __create_from_query(self):
        if self.__join_tables_involved:
            self.__switch_to_join_query()
        else:
            proxy_name = self.__base_table_proxy
            if self.__delete:
                self.__from_query = "FROM {} AS {}".format(
                    self.__full_table_name,
                    proxy_name
                )
            else:
                self.__column_query = ", ".join(["{}.{}".format(proxy_name, i) for i in self.__table_columns])
                self.__from_query = "{} FROM {}".format(
                    self.__column_query,
                    "{} AS {}".format(self.__full_table_name, proxy_name)
                )

    def __create_order_by_query(self):
        order_query = ""
        if not self.__delete:
            if self.__order_dict:
                order_query = ", ".join(
                    ["{}.{} {}".format(self.__base_table_proxy, k, v) for k, v in self.__order_dict.items()])
            if order_query:
                order_query = " ORDER BY {}".format(order_query)
        self.__order_by_query = order_query

    def __create_limit_offset_query(self):
        query = ""
        if self.__limit is not None:
            query += " LIMIT {}".format(self.__limit)
        if self.__offset:
            query += " OFFSET {}".format(self.__offset)
        self.__limit_offset_query = query

    def query(self):

        self.__create_where_query()
        self.__process_select_related()
        self.__create_from_query()

        self.__create_order_by_query()
        self.__create_limit_offset_query()

        query = (
            self.__from_query +
            self.__where_query +
            self.__order_by_query +
            self.__limit_offset_query
        )
        return (
            self.__base_query.format(query),
            self.__params,
            self.__column_query,
            self.__table_details,
            self.__base_table_proxy
        )
