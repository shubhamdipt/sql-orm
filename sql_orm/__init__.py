import inspect
from copy import deepcopy


DATABASE_TYPES = {
    "PostgreSQL": "postgresql",
}


class SQLException(Exception):
    pass


NON_COLUMN_FIELDS = ("__module__", "__doc__", "Meta", "_schema")


class BaseField:

    def __deepcopy__(self, memodict):
        cls = self.__class__
        result = cls.__new__(cls)
        for k, v in self.__dict__.items():
            setattr(result, k, deepcopy(v))
        return result


class Table:

    database_type = None

    def __init__(self, database_type, **kwargs):
        if database_type not in DATABASE_TYPES.values():
            raise SQLException("Wrong database_type. Valid options: {}".format(", ".join(DATABASE_TYPES.values())))
        self.database_type = database_type
        for i in self.__class__.get_column_names():
            self.__dict__[i] = kwargs.get(i)

    def __getattribute__(self, item):
        if item.startswith("__"):
            return object.__getattribute__(self, item)
        try:
            return self.__dict__[item]
        except KeyError:
            return object.__getattribute__(self, item)

    def __setattr__(self, key, value):
        if key.startswith("__"):
            object.__setattr__(self, key, value)
        try:
            self.__dict__[key] = value
        except KeyError:
            object.__setattr__(self, key, value)

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
                if k != k.lower():
                    raise SQLException("Column names should be in lowercase.")
                names.append(k)
        return sorted(names)

    @classmethod
    def get_table_name(cls):
        return cls.__name__.lower()


class FKFieldTree:

    def __init__(self, name='root', children=None, parent=None):
        self.name = name
        self.children = []
        self.parent = None
        if parent:
            self.set_parent(parent)
        if children:
            for child in children:
                self.add_child(child)

    def __repr__(self):
        return self.name

    def add_child(self, node):
        if not isinstance(node, FKFieldTree):
            raise ValueError("The child should be a FieldTree instance.")
        self.children.append(node)

    def set_parent(self, node):
        if not isinstance(node, FKFieldTree):
            raise ValueError("The parent should be a FieldTree instance.")
        self.parent = node
