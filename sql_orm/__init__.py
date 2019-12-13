DATABASE_TYPES = {
    "PostgreSQL": "postgresql",
}


class SQLException(Exception):
    pass


NON_COLUMN_FIELDS = ("__module__", "__doc__", "Meta", "_schema")


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
        fields = {}
        for k, v in cls.__dict__.items():
            if k not in NON_COLUMN_FIELDS:
                fields[k] = v
        return fields

    @classmethod
    def _get_meta_field(cls):
        return cls.__dict__.get("Meta")

    @classmethod
    def get_column_names(cls):
        return [k for k in cls.__dict__.keys() if k not in NON_COLUMN_FIELDS]

    @classmethod
    def get_table_name(cls):
        return cls.__name__.lower()
