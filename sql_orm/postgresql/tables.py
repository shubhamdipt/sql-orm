from sql_orm import postgresql, DATABASE_TYPES, SQLException, Table
from sql_orm.postgresql import sql
from sql_orm.postgresql.objects import Objects


class PostgreSQLTable(Table):

    _schema = "public"
    objects = Objects()

    def __init__(self, **kwargs):
        super().__init__(database_type=DATABASE_TYPES["PostgreSQL"], **kwargs)

    @classmethod
    def get_schema(cls):
        return cls._schema

    @classmethod
    def __create_schema(cls):
        return sql.create_schema().format(name=cls.get_schema())

    @classmethod
    def __create_table(cls):
        add_primary_key = ""
        for k, v in cls._get_column_fields().items():
            if v.primary_key:
                if not add_primary_key:
                    add_primary_key = "{name} {field_type} {properties}".format(
                        name=k,
                        field_type=v.field_type,
                        properties=v.properties
                    )
                else:
                    raise SQLException("Multiple primary keys found.")
        if not add_primary_key:
            raise SQLException("No primary key found.")
        return sql.create_table().format(
            schema=cls.get_schema(),
            name=cls.get_table_name(),
            add_primary_key=add_primary_key
        )

    @classmethod
    def __create_columns(cls):
        schema = cls.get_schema()
        table_name = cls.get_table_name()
        for k, v in cls._get_column_fields().items():
            if v.primary_key:
                continue
            if v.database_type != DATABASE_TYPES["PostgreSQL"]:
                raise SQLException("Mismatch database fields and table types. Field: ")
            yield v.create(schema=schema, table_name=table_name, column_name=k)

    @classmethod
    def __create_meta_properties(cls):
        meta_queries = []
        meta_field = cls._get_meta_field()
        column_names = cls.get_column_names()
        if meta_field:
            unique_together = meta_field.__dict__.get("unique_together", ())
            for i in unique_together:
                for j in i:
                    if j not in column_names:
                        raise SQLException("Column {} does not exist in the model {}.".format(j, cls.get_table_name()))
                meta_queries.append(sql.add_unique_together().format(
                    schema=cls.get_schema(),
                    table_name=cls.get_table_name(),
                    constraint_name="{}_uniq".format("_".join(i)),
                    columns=",".join(i)
                ))
        return meta_queries

    @classmethod
    def get_value_or_object_pk(cls, value):
        return getattr(value, "pk") if hasattr(value, "pk") else value

    def __get_field_value(self, field_name):
        value = getattr(self, field_name)
        return self.__class__.get_value_or_object_pk(value)

    @classmethod
    def migrate(cls, dry_run=False):
        queries = (
            ["BEGIN;", cls.__create_schema(), cls.__create_table()] +
            [i for i in cls.__create_columns()] +
            cls.__create_meta_properties() +
            ["COMMIT;"]
        )
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
            pk_name = self.__class__.get_pk_name()
            column_names = [i for i in self.__class__.get_column_names() if i != pk_name]
            params = [self.__get_field_value(i) for i in column_names] + [self.pk]
            query = sql.update_table_row().format(
                schema=self.__class__.get_schema(),
                table_name=self.__class__.get_table_name(),
                set_key_value=", ".join(["{}=%s".format(i) for i in column_names]),
                condition="{}=%s".format(pk_name)
            )
            with postgresql.PostgreSQL() as pgsql:
                pgsql.query(query, params=params)
                pgsql.commit()
        else:
            column_names = [i for i in self.__class__.get_column_names() if i != "id"]
            params = [self.__get_field_value(i) for i in column_names]
            query = sql.insert_table_row().format(
                schema=self.__class__.get_schema(),
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

    def delete(self):
        if getattr(self, "pk"):
            self.__class__.objects.filter(pk=self.pk).delete()
        else:
            raise SQLException("Missing primary key for the given object.")

    def as_dict(self):
        return {k: getattr(self, k) for k in self.__class__.get_column_names()}