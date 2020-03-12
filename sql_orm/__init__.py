import inspect


DATABASE_TYPES = {
    "PostgreSQL": "postgresql",
}


class SQLException(Exception):
    pass


NON_COLUMN_FIELDS = ("__module__", "__doc__", "Meta", "_schema")


class BaseField:
    pass


class Table:

    database_type = None

    def __init__(self, database_type, **kwargs):
        if database_type not in DATABASE_TYPES.values():
            raise SQLException("Wrong database_type. Valid options: {}".format(", ".join(DATABASE_TYPES.values())))
        self.database_type = database_type
        for i in self.__class__.get_column_names():
            setattr(self, i, kwargs.get(i))

    @classmethod
    def _get_column_fields(cls):
        valid_column_names = cls.get_column_names()
        return {k: v for k, v in cls.__dict__.items() if k in valid_column_names}

    @classmethod
    def _get_meta_field(cls):
        return cls.__dict__.get("Meta")

    @classmethod
    def get_column_names(cls):
        names = []
        for k, v in cls.__dict__.items():
            class_inspects = inspect.getmro(v.__class__)
            if len(class_inspects) > 1 and class_inspects[-2] == BaseField:
                names.append(k)
        return sorted(names)

    @classmethod
    def get_table_name(cls):
        return cls.__name__.lower()
