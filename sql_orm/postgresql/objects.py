from sql_orm import postgresql
from sql_orm.postgresql import sql
from sql_orm.postgresql import datatypes


class QueryException(Exception):
    pass


class ObjectDoesNotExist(Exception):
    pass


class MultipleObjectsFound(Exception):
    pass


class RowSet:

    def __init__(self, table_class):
        self.pgsql = postgresql.PostgreSQL()
        self.__table_class = table_class
        self.__table_columns = table_class.get_column_names()
        self.__filter_exclude_inputs = {
            "filter": {},
            "or_filter": {},
            "exclude": {},
            "order_by": {}
        }
        self.__limit = None
        self.__offset = None
        self.__delete = False
        self.__select_related = []
        self.__columns_order = []
        self.__value = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        self.pgsql.close()

    def __sql_read(self, query, params=()):
        for i in self.pgsql.fetch_query_results(query, params=params):
            yield i

    def __sql_delete(self, query, params=()):
        self.pgsql.query(query, params=params)
        self.pgsql.commit()

    def __update_query_inputs(self, data):
        if data:
            for k, v in data.items():
                self.__filter_exclude_inputs[k].update(v)

    def __create_query(self):
        query = sql.Query(
            schema=self.__table_class.get_schema(),
            table_class=self.__table_class,
            table_columns=self.__table_columns,
            pk=self.__table_class.get_pk_name(),
            order_dict=self.__filter_exclude_inputs["order_by"],
            filter_dict=self.__filter_exclude_inputs["filter"],
            or_filter_dict=self.__filter_exclude_inputs["or_filter"],
            exclude_dict=self.__filter_exclude_inputs["exclude"],
            select_related=self.__select_related,
            limit=self.__limit,
            offset=self.__offset,
            delete=self.__delete
        )
        sql_query, params, column_query = query.query()
        self.__columns_order = [i.strip().strip('"') for i in column_query.split(",")]
        return {"query": sql_query, "params": params}

    def __set_attributes(self, column_values):
        data_map = {self.__columns_order[i]: column_values[i] for i in
                    range(len(self.__columns_order))}

        def model_obj_set_attr(obj_table_class, model_obj, fk_field_name=None):
            model_schema = obj_table_class.get_schema()
            model_name = obj_table_class.get_table_name()
            for obj_field in obj_table_class.get_column_names():
                this_field = "{}.{}.{}".format(model_schema, model_name, obj_field)
                if this_field not in data_map:
                    return data_map[fk_field_name]
                if isinstance(obj_table_class.__dict__[obj_field], datatypes.ForeignKeyField):
                    obj_fk_table_class = getattr(obj_table_class.__dict__[obj_field], "table_name")
                    mdoel_fk_obj = obj_fk_table_class()
                    model_fk_obj = model_obj_set_attr(obj_fk_table_class, mdoel_fk_obj, fk_field_name=this_field)
                    setattr(model_obj, obj_field, model_fk_obj)
                else:
                    setattr(model_obj, obj_field, data_map[this_field])
            return model_obj

        schema = self.__table_class.get_schema()
        table_name = self.__table_class.get_table_name()
        obj = self.__table_class()
        if len(self.__table_columns) == len(self.__columns_order):
            for k in self.__table_columns:
                field = "{}.{}.{}".format(schema, table_name, k)
                setattr(obj, k, data_map[field])
        elif len(self.__table_columns) < len(self.__columns_order):
            obj = model_obj_set_attr(self.__table_class, obj)
        return obj

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
            return [i for i in self.__iter__()][0]
        raise ValueError("Invalid index.")

    def __iter__(self):
        if self.__value is None:
            for i in self.__sql_read(**self.__create_query()):
                obj = self.__set_attributes(i)
                yield obj
        else:
            for i in self.__value:
                yield i

    def __next__(self):
        return next(self.__iter__())

    def __validate_kwargs(self, kwargs):
        for key in kwargs.keys():
            column_name = key.rsplit(sql.LOGICAL_SEPARATOR, 1)[0]
            if column_name not in self.__table_columns:
                raise ValueError("The column could not be found. {}".format(column_name))

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

    def or_filter(self, **kwargs):
        self.__update_query_inputs({"or_filter": kwargs})
        return self

    def exclude(self, **kwargs):
        self.__update_query_inputs({"exclude": kwargs})
        return self

    def select_related(self, field=None):
        if field:
            self.__select_related.append(field)
        else:
            self.__select_related = [k for k, v in self.__table_class.__dict__.items() if isinstance(v, datatypes.ForeignKeyField)]
        return self

    def get(self, **kwargs):
        self.__filter_exclude_inputs["filter"] = kwargs
        objects_found = [i for i in self.__iter__()]
        if not objects_found:
            raise ObjectDoesNotExist("Object does not exist.")
        if len(objects_found) > 1:
            raise MultipleObjectsFound("Multiple objects found.")
        return objects_found[0]

    def get_or_create(self, **kwargs):
        created = False
        try:
            obj = self.get(**kwargs)
        except ObjectDoesNotExist:
            obj = self.create(**kwargs)
            created = True
        except MultipleObjectsFound as e:
            raise e
        return obj, created

    def get_or_none(self, **kwargs):
        try:
            obj = self.get(**kwargs)
            return obj
        except Exception as e:
            pass
        return None

    def delete(self):
        self.__delete = True
        self.__sql_delete(**self.__create_query())

    def count(self):
        self.__value = [i for i in self]
        return len(self.__value) if self.__value else 0


class Objects:

    def __get__(self, instance, owner):
        self.row_set = RowSet(
            table_class=owner,
        )
        return self.row_set
