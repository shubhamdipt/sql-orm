from collections import OrderedDict


class InvalidQueryException(Exception):
    pass


def create_schema():
    return "CREATE SCHEMA IF NOT EXISTS {name};"


def create_table():
    return "CREATE TABLE IF NOT EXISTS {schema}.{name} ({add_primary_key});"


def add_table_column():
    return "ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS {name} {field_type} {properties};"


def insert_table_row():
    return "INSERT INTO {schema}.{table_name} ({column_names}) VALUES ({column_values}) RETURNING id;"


def update_table_row():
    return "UPDATE {schema}.{table_name} SET {set_key_value} WHERE {condition};"


def add_unique_together():
    return "ALTER TABLE {schema}.{table_name} ADD CONSTRAINT {constraint_name} UNIQUE ({columns});"


LOGICAL_SEPARATOR = "__"


class Query:

    def __init__(self, schema, table_class, table_columns, pk, order_dict=None, filter_dict=None, or_filter_dict=None, exclude_dict=None, limit=None, offset=None, delete=False):
        self.__schema = schema
        self.__table_class = table_class
        self.__table_name = table_class.get_table_name()
        self.__table_columns = table_columns
        self.__pk = pk
        self.__order_dict = order_dict if order_dict else {"id": "ASC"}
        self.__filter_dict = filter_dict
        self.__or_filter_dict = or_filter_dict
        self.__exclude_dict = exclude_dict
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
        }
        self.__params = []
        self.__base_query = "SELECT {};" if not delete else "DELETE {};"
        self.__from_query = ""
        self.__where_query = ""
        self.__order_by_query = ""
        self.__limit_offset_query = ""
        self.__tables_involved = OrderedDict()

    def __logical_conditions(self, schema, table_name, key, value, condition):
        if key == "pk":
            key = self.__pk
        key = "{}.{}.{}".format(schema, table_name, key)
        if condition == "=":
            self.__params.append(value)
            return "{key}=%s".format(key=key)
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
                value=value,
            )
        self.__params.append(value)
        return "{key}{operation}%s".format(
            key=key,
            operation=self.__operators[condition],
        )

    def __set_tables_involved(self, table_class, key=None):
        table = "{}.{}".format(table_class.get_schema(), table_class.get_table_name())
        if table in self.__tables_involved:
            if key and not self.__tables_involved[table]["key"]:
                self.__tables_involved[table].update({"key": key})
        else:
            self.__tables_involved[table] = {
                "schema": table_class.get_schema(),
                "table": table_class.get_table_name(),
                "columns": table_class.get_column_names(),
                "pk": table_class.get_pk_name(),
                "key": key,
            }

    def __switch_to_join_query(self):
        tables = list(self.__tables_involved.items())
        if self.__delete:
            from_query = "FROM {} USING ".format(tables[0][0])
            self.__from_query = from_query + ", ".join([i[0] for i in tables[1:]])
        else:
            column_query = ", ".join(",".join(["{}.{}.{}".format(i["schema"], i["table"], j) for j in i["columns"]]) for _, i in tables)
            join_query = "{} FROM {}.{}".format(column_query, tables[0][1]["schema"], tables[0][1]["table"])
            for i in range(1, len(tables)):
                join_query += (
                    " LEFT JOIN " +
                    "{}.{}".format(tables[i][1]["schema"], tables[i][1]["table"]) +
                    " ON " +
                    "{}={}".format("{}.{}.{}".format(tables[i-1][1]["schema"], tables[i-1][1]["table"], tables[i-1][1]["key"]),
                                   "{}.{}.{}".format(tables[i][1]["schema"], tables[i][1]["table"], tables[i][1]["pk"]))
                )
            self.__from_query = join_query

    def __change_to_sql_conditions(self, key, value):
        key_splits = key.split(LOGICAL_SEPARATOR)
        if len(key_splits) == 1:
            self.__set_tables_involved(table_class=self.__table_class)
            return self.__logical_conditions(
                schema=self.__schema,
                table_name=self.__table_name,
                key=key_splits[0],
                value=value,
                condition="="
            )
        else:
            if key_splits[-1] in self.__operators.keys():
                condition = key_splits[-1]
                key = key_splits[-2]
                fk_fields = key_splits[:-2]
                if not fk_fields:
                    self.__set_tables_involved(table_class=self.__table_class)
                    return self.__logical_conditions(
                        schema=self.__schema,
                        table_name=self.__table_name,
                        key=key,
                        value=value,
                        condition=condition
                    )
            else:
                key = key_splits[-1]
                condition = "="
                fk_fields = key_splits[:-1]

            fk_table_class = self.__table_class
            self.__set_tables_involved(table_class=self.__table_class, key=fk_fields[0])
            for i in range(len(fk_fields)):
                fk = fk_fields[i]
                if not hasattr(fk_table_class.__dict__[fk], "table_name"):
                    raise InvalidQueryException("Not a ForeignKey: {}".format(fk))
                fk_table_class = getattr(fk_table_class.__dict__[fk], "table_name")
                self.__set_tables_involved(table_class=fk_table_class, key=None if i == len(fk_fields) -1 else fk_fields[i+1])

            return self.__logical_conditions(
                schema=fk_table_class.get_schema(),
                table_name=fk_table_class.get_table_name(),
                key=key,
                value=value,
                condition=condition
            )

    def __create_where_query(self):
        filter_query = ""
        if self.__or_filter_dict:
            filter_query = "( {} )".format(" OR ".join([
                self.__change_to_sql_conditions(k, v)
                for k, v in self.__or_filter_dict.items()]))
        if self.__filter_dict:
            filter_query += " AND ".join([
                self.__change_to_sql_conditions(k, v)
                for k, v in self.__filter_dict.items()])
        if self.__exclude_dict:
            if self.__filter_dict:
                filter_query += " AND "
            filter_query += "NOT "
            filter_query += " AND NOT ".join([
                self.__change_to_sql_conditions(k, v)
                for k, v in self.__exclude_dict.items()])
        if filter_query:
            filter_query = " WHERE {}".format(filter_query)
        self.__where_query = filter_query

    def __create_from_query(self):
        if len(self.__tables_involved.keys()) <= 1:
            if self.__delete:
                self.__from_query = "FROM {}.{}".format(self.__schema, self.__table_name)
            else:
                self.__from_query = "{} FROM {}".format(
                    ", ".join(["{}.{}.{}".format(self.__schema, self.__table_name, i) for i in self.__table_columns]),
                    "{}.{}".format(self.__schema, self.__table_name)
                )
        else:
            self.__switch_to_join_query()

    def __create_order_by_query(self):
        order_query = ""
        if not self.__delete:
            if self.__order_dict:
                order_query = ", ".join(
                    ["{}.{}.{} {}".format(self.__schema, self.__table_name, k, v) for k, v in self.__order_dict.items()])
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
        self.__create_order_by_query()
        self.__create_limit_offset_query()

        self.__create_where_query()
        self.__create_from_query()

        query = (
            self.__from_query +
            self.__where_query +
            self.__order_by_query +
            self.__limit_offset_query
        )
        return self.__base_query.format(query), self.__params
