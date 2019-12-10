from sql_orm import postgresql
from sql_orm.postgresql import sql


class QueryException(Exception):
    pass


class RowSet:

    def __init__(self, table_class, table_name, table_columns):
        self.__table_class = table_class
        self.__table_name = table_name
        self.__table_columns = table_columns
        self.__filter_exclude_inputs = {
            "filter": {},
            "exclude": {},
            "order_by": {}
        }
        self.__limit = None
        self.__offset = None
        self.__delete = False

    @staticmethod
    def __sql_read(query, params=()):
        with postgresql.PostgreSQL() as pgsql:
            for i in pgsql.fetch_query_results(query, params=params):
                yield i

    @staticmethod
    def __sql_delete(query, params=()):
        with postgresql.PostgreSQL() as pgsql:
            pgsql.query(query, params=params)
            pgsql.commit()

    def __update_query_inputs(self, data):
        if data:
            for k, v in data.items():
                self.__filter_exclude_inputs[k].update(v)

    def __create_query(self):
        query = sql.Query(
            table_name=self.__table_name,
            table_columns=self.__table_columns,
            pk=self.__table_class.get_pk_name(),
            order_dict=self.__filter_exclude_inputs["order_by"],
            filter_dict=self.__filter_exclude_inputs["filter"],
            exclude_dict=self.__filter_exclude_inputs["exclude"],
            limit=self.__limit,
            offset=self.__offset,
            delete=self.__delete
        )
        sql_query = query.query()
        print(sql_query)
        return sql_query

    def __getitem__(self, index):
        if isinstance(index, slice):
            if index.step:
                print("WARNING: step provided will be ignored. Not yet supported in PostgreSQL")
            start = int(index.start) if index.start else 0
            if start < 0:
                raise ValueError("Start index cannot be negative.")
            self.__offset = start

            stop = int(index.stop) if index.stop else None
            if stop:
                if stop < start:
                    raise ValueError("Stop index cannot be negative and less than Start index.")
                self.__limit = stop - start
            return self.__iter__()
        if isinstance(index, int):
            if index < 0:
                raise ValueError("Index cannot be negative.")
            self.__limit = 1
            self.__offset = index
            return [i for i in self.__iter__()]
        raise ValueError("Invalid index.")

    def __iter__(self):
        for i in self.__sql_read(self.__create_query()):
            obj = self.__table_class()
            for col in range(len(self.__table_columns)):
                setattr(obj, self.__table_columns[col], i[col])
            yield obj

    def __next__(self):
        return next(self.__iter__())

    def create(self, **kwargs):
        obj = self.__table_class(**kwargs)
        obj.save(commit=True)
        return obj

    def order_by(self, params):
        data = {}
        if type(params) == str:
            params = (params, )
        for i in params:
            order = "DESC" if i[0] == "-" else "ASC"
            column_name = i[1:] if i[0] == "-" else i
            if column_name not in self.__table_columns:
                raise QueryException("Column not found: {}".format(column_name))
            data[column_name] = order
        self.__update_query_inputs({"order_by": data})
        return self

    def all(self):
        self.__update_query_inputs({})
        return self

    def filter(self, **kwargs):
        self.__update_query_inputs({"filter": kwargs})
        return self

    def exclude(self, **kwargs):
        self.__update_query_inputs({"exclude": kwargs})
        return self

    def get(self, **kwargs):
        self.__filter_exclude_inputs = {"filter": kwargs, "exclude": {},
                                        "order_by": {}}
        objects_found = [i for i in self.__iter__()]
        if not objects_found:
            raise QueryException("Object does not exist.")
        if len(objects_found) > 1:
            raise QueryException("Multiple objects found.")
        return objects_found[0]

    def delete(self):
        self.__delete = True
        self.__sql_delete(self.__create_query())


class Objects:

    def __get__(self, instance, owner):
        self.row_set = RowSet(
            table_class=owner,
            table_name=owner.get_table_name(),
            table_columns=owner.get_column_names()
        )
        return self.row_set
