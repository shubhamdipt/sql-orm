DATABASE_TYPES = {
    "PostgreSQL": "postgresql",
}


class SQLException(Exception):
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
        fields = {}
        for k, v in cls.__dict__.items():
            if k not in ["__module__", "__doc__"]:
                fields[k] = v
        return fields

    @classmethod
    def get_column_names(cls):
        return [v.name for k, v in cls.__dict__.items() if
                k not in ("__module__", "__doc__")]

    @classmethod
    def get_table_name(cls):
        return cls.__name__.lower()

    @classmethod
    def create_table(cls):
        pass

    @classmethod
    def create_columns(cls):
        pass
