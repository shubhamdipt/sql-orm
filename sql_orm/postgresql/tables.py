from sql_orm import postgresql, DATABASE_TYPES, SQLException, Table
from sql_orm.postgresql import sql
from sql_orm.postgresql.objects import Objects


class PostgreSQLTable(Table):

    objects = Objects()

    def __init__(self, **kwargs):
        super().__init__(database_type=DATABASE_TYPES["PostgreSQL"], **kwargs)

    @classmethod
    def create_table(cls):
        add_primary_key = ""
        for k, v in cls._get_column_fields().items():
            if v.primary_key:
                if not add_primary_key:
                    add_primary_key = "{name} {field_type} {properties}".format(
                        name=v.name,
                        field_type=v.field_type,
                        properties=v.properties
                    )
                else:
                    raise SQLException("Multiple primary keys found.")
        if not add_primary_key:
            raise SQLException("No primary key found.")
        return sql.create_table().format(
            name=cls.get_table_name(),
            add_primary_key=add_primary_key
        )

    @classmethod
    def create_columns(cls):
        table_name = cls.__name__.lower()
        for k, v in cls._get_column_fields().items():
            if v.primary_key:
                continue
            if v.database_type != DATABASE_TYPES["PostgreSQL"]:
                raise SQLException("Mismatch database fields and table types. Field: ")
            yield v.create(table_name=table_name)

    @classmethod
    def migrate(cls, dry_run=False):
        queries = [cls.create_table()] + [i for i in cls.create_columns()]
        with postgresql.PostgreSQL() as pgsql:
            for query in queries:
                if dry_run:
                    print(pgsql.mogrify(query))
                else:
                    pgsql.query(query)
                    pgsql.commit()

    @property
    def pk(self):
        for k, v in self.__class__._get_column_fields().items():
            if v.primary_key:
                return getattr(self, k)

    @classmethod
    def get_pk_name(cls):
        for k, v in cls._get_column_fields().items():
            if v.primary_key:
                return k

    def _sql_save(self, commit=True):
        if getattr(self, "pk"):
            raise
            pk_name = self.__class__.get_pk_name()
            column_names = [i for i in self.__class__.get_column_names() if i != pk_name]
            params = [getattr(self, i) for i in column_names] + [self.pk]
            query = sql.update_table_row().format(
                table_name=self.__class__.get_table_name(),
                set_key_value=", ".join(["{}=%s".format(i) for i in column_names]),
                condition="{}=%s".format(pk_name)
            )
            with postgresql.PostgreSQL() as pgsql:
                pgsql.query(query, params=params)
                pgsql.commit()
        else:
            column_names = [i for i in self.__class__.get_column_names() if i != "id"]
            params = [getattr(self, i) for i in column_names]
            query = sql.insert_table_row().format(
                table_name=self.__class__.get_table_name(),
                column_names=", ".join(column_names),
                column_values=", ".join(["%s"] * len(column_names))
            )
            obj_id = None
            if commit:
                with postgresql.PostgreSQL() as pgsql:
                    pgsql.query(query, params=params)
                    obj_id = pgsql.fetchone()[0]
                    pgsql.commit()
            setattr(self, "id", obj_id)

    def save(self, commit=True):
        self._sql_save(commit=commit)

    def as_dict(self):
        return {k: getattr(self, k) for k in self.__class__.get_column_names()}