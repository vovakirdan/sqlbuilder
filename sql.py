from __future__ import annotations
import warnings
from sqlalchemy import text
from typing import TypeVar, Any, Iterable, Literal
from json import dumps
import sqlparse
from numpy import isnan
import re
from datetime import datetime


def is_datetime(obj: Any) -> bool:
    return isinstance(obj, datetime)


class SqlCreator:
    # todo: drop col, drop row
    __doc__ = __docstring__ = \
        """
        If you want more classical and less-userfriendly way see https://docs.sqlalchemy.org/en/14/orm/tutorial.html
    Is a class-helper for amount queries. You can easily construct any amount query with any parameters.
    Call engine() function

    Set concatenating_where_default parameter to choose what kind of operator will be used ("AND" or "OR" etc.)

    Set substitute parameter to create executable query by substitute parameters.
    :param schema: schema to connect. If you pass empty string, script will return only table (not schema.table const.)
    :param table: table to connect
    :param where: may be a dict like: {comparison operator: {obj1: obj2}, "OR obj2 = '3'": None, etc.}
                  This way returns you a string like: WHERE obj1 comparison operator obj2 OR obj = '3' etc...
                  Be careful, static operator for concatenating is AND operator.
                  You can set it by property self.concatenating_where_default = 'your operator'.
                  OR
                  may be a ready_to_go string: WHERE t1 <= t2. Of course, it returns original string.
    -----------------------------------------------------------------------------------------------------------------
    USAGE:
    SqlCreator:
        query = SqlCreator(schema='banks', table='ras_forms')
        query.concatenating_where_default = "OR"
        query.where = {
            '=': {'obj1': 'obj2', 'obj3': 'obj4'},
            '<=': {'obj': 44},
            'AND first is not None': None
        }
        # let's see what we have
        print(query.select('uuid', 'file'))
        >>>SELECT uuid, file FROM banks.ras_forms WHERE obj1='obj2' OR obj3='obj4' OR obj<=44 AND first is not None

        # reset where
        query.where = "WHERE second IS NOT NULL"
        print(query.select())
        >>>SELECT * FROM banks.ras_forms WHERE second IS NOT NULL

        # see insert query
        print(query.insert())  # and we have warning because columns and values are not passed
        >>>INSERT INTO banks.ras_forms() VALUES() WHERE second IS NOT NULL
            SyntaxWarning: No arguments passed! Query won't be effective

        # then set substitute parameter
        query.substitute = True
        print(query.insert(lic=600, form='0409115'))  # and so you can call engine.execute(query, params)
        >>>INSERT INTO banks.ras_forms(lic, form) VALUES(%s, %s) WHERE second IS NOT NULL

        # see the update script
        query.substitute = False
        print(query.update(lic=600, form='0409115'))
        >>>UPDATE banks.ras_forms SET lic=600, form='0409115' WHERE second IS NOT NULL
"""

    def __init__(
            self,
            schema: str = 'banks',
            table: str = '',
            where: dict | str = None,
            **kwargs
    ):
        # substitute: bool = False,
        # comma: bool = False
        self._schema = schema
        self._table = table
        self._where = where
        self._concatenating_where_default = ' AND '
        self._substitute = kwargs.get('substitute', False)
        self._comma = ('', ';')[kwargs.get('comma', False)]

    @property
    def schema(self) -> str:
        return self._schema

    @property
    def table(self) -> str:
        return self._table

    @property
    def _dot(self) -> str:
        return '.' if self.schema else ''

    @property
    def where(self) -> dict:
        return self._where

    @property
    def concatenating_where_default(self) -> str:
        return self._concatenating_where_default

    @property
    def substitute(self) -> bool:
        return self._substitute

    @property
    def comma(self) -> str:
        return self._comma

    @schema.setter
    def schema(self, value):
        self._schema = value

    @table.setter
    def table(self, value):
        self._table = value

    @where.setter
    def where(self, value):
        self._where = value

    @concatenating_where_default.setter
    def concatenating_where_default(self, value: str):
        self._concatenating_where_default = ' ' + value + ' '

    @substitute.setter
    def substitute(self, value: bool):
        self._substitute = value

    @comma.setter
    def comma(self, value: bool):
        self._comma = ('', ';')[value]

    @property
    def prepared_where(self) -> str:
        cases = self.where
        where_condition, append = '', ''
        if not cases:  # if nothing passed
            return where_condition
        where_condition += "WHERE "
        # too high structure...
        if type(cases) is dict:  # if passed a dict structure
            l = []
            for case, state in cases.items():  # case is comparing state is dict of objects
                if state is not None:
                    if type(state) != dict:
                        raise KeyError(f"Expected another dict structure for where parameter!")
                    for key, value in state.items():
                        if type(value) in (int, float):
                            if self._substitute:
                                l.append(f"{key}{case}%s")
                            else:
                                l.append(f"{key}{case}{value}")
                        else:
                            if self._substitute:
                                l.append(f"{key}{case}%s")
                            else:
                                l.append(f"{key}{case}'{value}'")
                else:
                    append += ' ' + case + ''
            where_condition += self._concatenating_where_default.join(l) + append
            l.clear()
        elif type(cases) is str:  # if passed ready string
            where_condition = cases
        return where_condition

    def select(self, *columns) -> str:
        """
        Create SELECT script
        :param columns: columns to select. If nothing passed, default is * (all columns)
        :return: correct SELECT script
        """
        where = self.prepared_where
        if not columns:
            columns = ['*']
        select_query = f"""SELECT {', '.join(columns)} FROM {self.schema}{self._dot}{self.table} """ + where
        return select_query + self.comma

    def insert(self, *c_v, **columns_values) -> str:
        insert_query = f'INSERT INTO {self.schema}{self._dot}{self.table}('
        if not columns_values and c_v:
            columns_values = {column: "%s" for column in c_v}
        columns = [str(column) for column in columns_values.keys()]
        # (f"{value}", f"'{value}'")[type(value) == str] this construction chooses a comma if it is str
        values = ['%s' if self._substitute else (f"{value}", f"'{value}'")[type(value) == str and not value == '%s']
                  for value in columns_values.values()]
        if len(columns_values) == 0 and len(c_v) == 0:
            warnings.warn("No arguments passed! Query won't be effective", SyntaxWarning, stacklevel=2)
        insert_query += ', '.join(columns) + ') VALUES('
        insert_query += ', '.join(values) + ') '
        where = self.prepared_where
        return insert_query + where + self.comma

    def update(self, *c_v, **columns_values) -> str:
        where = self.prepared_where
        update_query = f"""UPDATE {self.schema}{self._dot}{self.table} SET """
        if len(columns_values) == 0 and len(c_v) == 0:
            warnings.warn("No arguments passed! Query won't be effective", SyntaxWarning, stacklevel=2)
        l = []
        if not columns_values and c_v:
            columns_values = {column: "%s" for column in c_v}
        for column, value in columns_values.items():
            v = (f"{value}", f"'{value}'")[type(value) == str and not value == '%s']
            if self._substitute:
                v = '%s'
            l.append(f"{column}=" + v)
        update_query += ', '.join(l) + ' '
        return update_query + where + self.comma

    def delete(self) -> str:
        where = self.prepared_where
        delete_query = f"""DELETE FROM {self.schema}{self._dot}{self.table} """
        return delete_query + where + self.comma

    def create_table(self, owner: str = 'developer', **columns: str) -> str:
        """
        Generates a correct SQL CREATE TABLE query string.

        Parameters:
        :param owner: OWNER to owner
        :param columns: (list): a list of dictionaries representing the columns in the table.
         Each dictionary should contain the keys "name" and "data_type",
          representing the name and data type of the column, respectively
        :return:
        - str: the generated CREATE TABLE query.
        """
        comments = f'-- Table: {self.schema}{self._dot}{self.table}\n\n'

        query = comments + f"CREATE TABLE IF NOT EXISTS {self.schema}{self._dot}{self.table}\n(\n"

        # Format the columns
        column_def = ',\n'.join([f'{column_name} {column_type}' for column_name, column_type in columns.items()])
        query += column_def + '\n)\n'

        query += f"""TABLESPACE pg_default{self.comma}
        ALTER TABLE IF EXISTS {self.schema}{self._dot}{self.table}
        OWNER to {owner}{self.comma}"""

        return query

    def truncate(self, *tables, restart_identity: bool = False, cascade: bool = False) -> str:
        """
        Return truncate string
        :param tables: add tables_names to script. If empty return only default string
        :param restart_identity: add RESTART IDENTITY
        :param cascade: add CASCADE
        :return: Truncate script
        """
        truncate_script = f"TRUNCATE "
        if tables:
            truncate_script += ', '.join(tables)
        else:
            truncate_script += f"{self.schema}{self._dot}{self.table}"
        if restart_identity:
            truncate_script += ' RESTART IDENTITY'
        if cascade:
            truncate_script += ' CASCADE'
        return truncate_script + self.comma


class Where:
    def __init__(self, *args):
        self._where_conditions = []

    def where(self, condition):
        self._where_conditions.append(condition)
        return self

    def and_where(self, condition):
        self._where_conditions.append(('AND', condition))
        return self

    def or_where(self, condition):
        self._where_conditions.append(('OR', condition))
        return self

    def in_where(self, column, values):
        condition = f"{column} IN ({','.join(map(str, values))})"
        self._where_conditions.append(condition)
        return self

    def is_where(self, column, value):
        condition = f"{column} IS {value}"
        self._where_conditions.append(condition)
        return self

    def __repr__(self):
        query = ""
        if self._where_conditions:
            query += "WHERE "
            for condition in self._where_conditions:
                if isinstance(condition, str):
                    query += f"{condition} "
                elif isinstance(condition, tuple) and len(condition) == 2:
                    operator, condition_str = condition
                    query += f"{operator} {condition_str} "
        return query.strip()


class WhereError(Exception):
    """Custom error says that there are no "where" clause"""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class QueryError(Exception):
    """
    Invalid query exception
    """

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class Select(Where):
    """not used at the moment"""

    def __init__(self, *columns, **kwargs):
        super().__init__(*columns)
        self.schema = kwargs.get('schema', '')
        self.table = kwargs.get('table', '')
        self.columns = ['*'] if columns == () else columns

    def __repr__(self):
        string = f"""SELECT {', '.join(self.columns)} FROM {self.schema}.{self.table};"""
        return string


def cast_types(value: Any) -> str:
    if isinstance(value, str):
        return f"CAST('{value}' AS TEXT)"
    elif isinstance(value, int):
        return f"CAST({value} AS INTEGER)"
    elif isinstance(value, float):
        return f"CAST({value} AS FLOAT)"
    elif isinstance(value, bool):
        return f"CAST({value} AS BOOLEAN)"
    elif isinstance(value, dict):
        json_str = dumps(value)
        return f"CAST('{json_str}' AS JSONB)"
    # elif isinstance(value, datetime):
    #     dt_str = value.strftime('%Y-%m-%d %H:%M:%S')
    #     return f"CAST('{dt_str}' AS DATETIME)"
    else:
        return f"CAST('{value}' AS TEXT)"


def proper_type(value: Any, force_str: bool = False) -> str | int:
    if value is None:
        return 'NULL'
    if isinstance(value, str):
        if value.lower() == 'null':
            return 'NULL'
        elif value == '':
            return 'NULL'
        if force_str:
            return f"'{value}'"
        return f'{value}'
    if isinstance(value, int):
        if force_str:
            return f'{value}'
        return value
    if isinstance(value, float):
        if force_str:
            return f"'{value}'" if not isnan(value) else 'NULL'
        return str(value) if not isnan(value) else 'NULL'
    if isinstance(value, bool):
        return f"{value}"
    if isinstance(value, dict):
        json_str = dumps(value)
        if force_str:
            return f"'{json_str}'"
        return json_str
    if is_datetime(value):
        return value
    return f"'{value}'" if force_str else f'{value}'


class SqlCreatorAlpha(Where):
    __doc__ = """sqlalchemy 2.x ready
    It's new, improved version of SQL string builder.
    Remember the easy-to-use pattern:

    class.<action>[.where[.or_where/and_where/in_where/is_where]]

    SELECT
    You can build any amount clause with multiple where clause:

    q = SqlCreatorAlpha().select("name", "age").where("age > 18").and_where("gender = 'female'")
    print(q)
    >>>SELECT name, age WHERE age > 18 AND gender = 'female'

    INSERT
    (Of course, you can add here any WHERE clause, as for SELECT)

    q = SqlCreatorAlpha(schema="my_schema", table="my_table")
    q.insert({"name": "Alice", "age": 25})
    print(q)
    >>>INSERT INTO my_schema.my_table (name, age) VALUES ('Alice', 25)

    SET

    q = SqlCreatorAlpha(schema="my_schema", table="my_table")
    q.set({"name": "Alice", "age": 25}).where("id = 1")
    print(q)
    >>>UPDATE my_schema.my_table SET name=Alice, age=25 WHERE id = 1

    DELETE

    q = SqlCreatorAlpha(schema="my_schema", table="my_table")
    q.delete().where("age < 18").and_where("gender = 'male'")
    print(q)
    >>>DELETE FROM my_schema.my_table WHERE age < 18 AND gender = 'male'

    JOIN

    q = SqlCreatorAlpha(schema="my_schema", table="my_table")
    q.select('my_table.name', 'orders.order_date').join('orders', 'ras_forms._id = orders.custom_id')
    print(q)
    >>>SELECT my_table.name, orders.order_date FROM banks.ras_forms INNER JOIN banks.orders ON ras_forms._id = orders.custom_id

    LEFT JOIN

    q = SqlCreatorAlpha(schema="my_schema", table="my_table")
    q.select('my_table.name', 'orders.order_date').left_join('orders', 'ras_forms._id = orders.custom_id')
    print(q)
    >>>SELECT my_table.name, orders.order_date FROM banks.ras_forms LEFT JOIN banks.orders ON ras_forms._id = orders.custom_id

    RIGHT JOIN

    q = SqlCreatorAlpha(schema="my_schema", table="my_table")
    q.select('my_table.name', 'orders.order_date').right_join('orders', 'ras_forms._id = orders.custom_id')
    print(q)
    >>>SELECT my_table.name, orders.order_date FROM banks.ras_forms RIGHT JOIN banks.orders ON ras_forms._id = orders.custom_id

    FULL JOIN

    q = SqlCreatorAlpha(schema="my_schema", table="my_table")
    q.select('my_table.name', 'orders.order_date').full_join('orders', 'ras_forms._id = orders.custom_id')
    print(q)
    >>>SELECT my_table.name, orders.order_date FROM banks.ras_forms FULL JOIN banks.orders ON ras_forms._id = orders.custom_id

    JOIN with multiple conditions

    q = SqlCreatorAlpha(schema="my_schema", table="my_table")
    q.select('my_table.name', 'orders.order_date').join('orders', 'ras_forms._id = orders.custom_id', 'orders.order_date > 2021-01-01')
    print(q)
    >>>SELECT my_table.name, orders.order_date FROM banks.ras_forms INNER JOIN banks.orders ON ras_forms._id = orders.custom_id AND orders.order_date > 2021-01-01

    JOIN with multiple conditions and multiple tables

    q = SqlCreatorAlpha(schema="my_schema", table="my_table")
    q.select('my_table.name', 'orders.order_date').join('orders', 'ras_forms._id = orders.custom_id', 'orders.order_date > 2021-01-01', 'orders.order_date < 2021-01-31')
    print(q)
    >>>SELECT my_table.name, orders.order_date FROM banks.ras_forms INNER JOIN banks.orders ON ras_forms._id = orders.custom_id AND orders.order_date > 2021-01-01 AND orders.order_date < 2021-01-31

    WITH

    q = SqlCreatorAlpha(schema="my_schema", table="my_table")
    q.with_as('my_table', 'SELECT * FROM my_schema.my_table')
    print(q)
    >>>WITH my_table AS (SELECT * FROM my_schema.my_table)

    ORDER BY

    q = SqlCreatorAlpha(schema="my_schema", table="my_table")
    q.select('a', 'b', 'c').order_by('a', 'DESC')
    print(q)
    >>>SELECT a, b, c ORDER BY a DESC

    GROUP BY

    q = SqlCreatorAlpha(schema="my_schema", table="my_table")
    q.select('a', 'b', 'c').group_by('a', 'b')
    print(q)
    >>>SELECT a, b, c GROUP BY a, b

    LIMIT

    q = SqlCreatorAlpha(schema="my_schema", table="my_table")
    q.select('a', 'b', 'c').limit(10)
    print(q)
    >>>SELECT a, b, c LIMIT 10

    RETURNING

    q = SqlCreatorAlpha(schema="my_schema", table="my_table")
    q.insert({"name": "Alice", "age": 25}).returning('id')
    print(q)
    >>>INSERT INTO my_schema.my_table (name, age) VALUES ('Alice', 25) RETURNING id

    FROM RAW QUERY

    Will return normalized query string.

    q = SqlCreatorAlpha()
    q.from_raw('SELECT * FROM my_schema.my_table')
    print(q)
    >>>SELECT * FROM my_schema.my_table


    After all manipulations, Executor should call __build__ method to create right string.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self._schema = kwargs.get('schema', 'banks')
        self._table = kwargs.get('table', '')
        self.dot = self._dot()
        self.query = ''
        self._select_columns = []
        self._insert_columns = []
        self._insert_values = []
        self._update_columns = []
        self._set_values = []
        self._with_constructions = []
        self._order_by_columns = []
        self._order_by_state = ''
        self._group_by_columns = []
        self._join_clauses = []
        self._returning = ''
        self._limit = 0
        self._having = ''
        self._ignore_errors = kwargs.get('ignore_errors', False)
        self.__danger_query = False

    @property
    def schema(self) -> str:
        return self._schema

    @schema.setter
    def schema(self, value: str):
        self._schema = value

    @property
    def table(self) -> str:
        return self._table

    @table.setter
    def table(self, value: str | Iterable):
        self._table = value

    @property
    def ignore_errors(self) -> bool:
        return self._ignore_errors

    @ignore_errors.setter
    def ignore_errors(self, value: bool):
        self._ignore_errors = bool(value)
        if bool(value):
            print('Warnings will still be displayed!')

    def _dot(self):
        if self.schema is None or self.schema == '':
            return ''
        else:
            return '.'

    def select(self, *args: str):
        self._select_columns.extend(args)
        self._build_select()
        return self

    def _build_select(self):
        if not self._select_columns:
            self._select_columns.append("*")
        query = f"SELECT {', '.join(self._select_columns)} FROM {self.schema}{self._dot()}{self.table} "
        self.query = query

    def insert(self, column_values: dict[str, Any] = None, **c_v):
        if column_values is None:
            column_values = {}
        d = {}
        d.update({**column_values, **c_v})
        for k, v in d.items():
            d |= {k: proper_type(v)}
            # if isinstance(v, dict):
            #     d |= {k: dumps(v)}
        columns, values = zip(*d.items())
        self._insert_columns.extend(columns)
        self._insert_values.append(tuple(values))
        self._build_insert()
        return self

    def _build_insert(self):
        query = ""
        if self._insert_columns:
            query += f"INSERT INTO {self.schema}{self._dot()}{self.table} ({', '.join(self._insert_columns)}) "
            query += f"VALUES {', '.join([str(v) for v in self._insert_values])} "
        self.query = query

    def set(self, column_values: dict[str, Any] = None, **c_v):
        if column_values is None:  # maybe exists simpler way
            column_values = {}
        d = {}
        d.update({**column_values, **c_v})
        for k, v in d.items():
            d |= {k: proper_type(v, force_str=True)}
            # if isinstance(v, dict):
            #     d |= {k: f"'{dumps(v)}'"}
            # if isinstance(v, str):
            #     if len(v.split()) != 1:
            #         d |= {k: f"'{v}'"}
        columns, values = zip(*d.items())
        self._update_columns.extend(columns)
        self._set_values.append(tuple(values))
        self._build_set()
        return self

    def update(self, column_values: dict[str, Any] = None, **c_v):
        return self.set(column_values, **c_v)

    def _build_set(self):
        query = f"UPDATE {self.schema}{self._dot()}{self.table} "
        query += f"SET {', '.join([f'{col}={val}' for col, val in zip(self._update_columns, self._set_values[0])])} "
        self.query = query
        self.__danger_query = True

    def delete(self, *args, **kwargs):
        self._build_delete()
        return self

    def _build_delete(self):
        self.query = f"DELETE FROM {self.schema}{self._dot()}{self.table} "
        self.__danger_query = True

    def order_by(self, *columns: str, how: Literal['ASC', 'DESC'] = 'ASC'):
        self._order_by_columns.extend(columns)
        self._order_by_state = how
        return self

    def group_by(self, *columns: str):
        self._group_by_columns.extend(columns)
        return self

    def returning(self, *value: str):
        self._returning = ', '.join(value)
        return self

    def limit(self, lim: int):
        self._limit = lim
        return self

    def having(self, condition: str):
        self._having = condition
        return self

    def with_as(self, name: str = None, sql: SqlCreatorAlpha | str = None, **kwargs):
        if name:
            q = f"""{name} AS ({sql})"""
            self._with_constructions.append(q)
        for n, s in kwargs.items():
            self._with_constructions.append(f"""{n} AS ({s})""")
        return self

    def join(self, table, condition, join_type: Literal['LEFT', 'RIGHT', 'FULL', 'INNER', ''] = ""):
        join_clause = f"{join_type} JOIN {self.schema}{self._dot()}{table} ON {condition}"
        self._join_clauses.append(join_clause)
        return self

    def left_join(self, table, condition):
        return self.join(table, condition, join_type="LEFT")

    def right_join(self, table, condition):
        return self.join(table, condition, join_type="RIGHT")

    def full_join(self, table, condition):
        return self.join(table, condition, join_type="FULL")

    def build_join(self) -> str:
        if self._join_clauses:
            return " ".join(self._join_clauses)
        else:
            return ""

    @staticmethod
    def format_query(query, **format_args) -> str:
        """
        Format query for execution
        """
        # null trimming
        pattern = re.compile(r"\'null\'", re.M | re.I)
        if isinstance(query, SqlCreatorAlpha):
            query = query.__repr__()
        query = re.sub(pattern, 'NULL', query)
        if not format_args.get("reindent"):
            format_args["reindent"] = True
        if not format_args.get("keyword_case"):
            format_args["keyword_case"] = "upper"
        return sqlparse.format(query, **format_args)

    @staticmethod
    def _validate_query(query) -> bool:
        if isinstance(query, SqlCreatorAlpha):
            query = query.__repr__()
        parsed = sqlparse.parse(query)
        if len(parsed) == 0:
            raise QueryError("Empty query")
        elif len(parsed) > 1:
            raise QueryError("Multiple queries in one statement are not supported")
        else:
            stmt = parsed[0]
            if stmt.get_type() not in ["SELECT", "INSERT", "UPDATE", "DELETE"]:
                raise QueryError("Invalid query type")
            elif not stmt.is_group:
                raise QueryError("Malformed query")
            else:
                return True

    @classmethod
    def from_raw(cls, query: str):
        """
        Prepare raw amount query for execution
        :param query: raw amount query
        """
        c = cls()
        if c._validate_query(query):
            # cls.clear()
            c.query = c.format_query(query)
        return c

    def __repr__(self) -> str:
        query = self.query
        if self._with_constructions:
            q = 'WITH ' + ',\n'.join(self._with_constructions)
            query = q + '\n' + query
        if self._join_clauses:
            query += self.build_join()
        if self._where_conditions:
            query += "\nWHERE "
            for condition in self._where_conditions:
                if isinstance(condition, str):
                    query += f"{condition} "
                elif isinstance(condition, tuple) and len(condition) == 2:
                    operator, condition_str = condition
                    query += f"{operator} {condition_str} "
        if self._group_by_columns:
            query += f"\nGROUP BY {', '.join(self._group_by_columns)}"
        if self._order_by_columns:
            query += f"\nORDER BY {', '.join(self._order_by_columns)} {self._order_by_state}"
        if self._having:
            query += f"\nHAVING {self._having}"
        if self._limit != 0:
            query += f"\nLIMIT {self._limit}"
        if self._returning:
            query += '\nRETURNING ' + self._returning
        if self.__danger_query and not self._where_conditions and not self._ignore_errors:
            raise WhereError(
                '''\nIt seems you forgot to use .where method.\nTo turn off this exception set .ignore_errors = True''')
        if self.__danger_query and not self._where_conditions and self._ignore_errors:
            warnings.warn('It seems you forgot to use .where method.', stacklevel=2)
        return self.format_query(query)  # think about it

    def clear(self):
        for key in self.__dict__.keys():
            if key != '_schema' and key != '_table':
                if isinstance(self.__dict__[key], list):
                    self.__dict__[key].clear()
                elif isinstance(self.__dict__[key], int):
                    self.__dict__[key] = 0
                elif isinstance(self.__dict__[key], bool):
                    self.__dict__[key] = False
                else:
                    self.__dict__[key] = ''

    def __build__(self):
        return text(str(self))


SqlConstructor = TypeVar('SqlConstructor', SqlCreator, SqlCreatorAlpha, str)

if __name__ == "__main__":
    amount = SqlCreatorAlpha(schema='banks', table='ras_forms_115')
    amount.select('COUNT(*) as c', 'license', '_date', 'uuid_2').group_by('license', '_date', 'uuid_2').order_by('c',
                                                                                                                 how='DESC')

    sql = SqlCreatorAlpha(schema='', table='amount')
    sql.with_as('amount', amount).select('c', 'license', '_date', 'meta').join(
        'level_1', 'amount.uuid_2 = level_1.uuid_'
    ).join(
        'filestorage', 'level_1.parent = filestorage.uuid_'
    ).where('c < 1500')

    print(sql)
    # data = executor.execute(amount)

    h = 6
