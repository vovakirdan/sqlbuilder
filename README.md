# SQLQueryConstructor class
## The SQLQueryConstructor class is a Python class that can be used to construct SQL queries for select, insert, update, and delete operations. It allows you to build SQL queries in a programmatic way, with a fluent syntax that is easy to read and write.
## *It's new, improved version of SQL string builder. Remember the easy-to-use pattern:*
    
### `class.<action>[.where[.or_where/and_where/in_where/is_where]][.returning][.order_by]`
### Creating a new query
To create a new query, you can create an instance of the SQLQueryConstructor class and specify the schema and table that you want to operate on:

```python3
from tools import SqlCreatorAlpha
sql = SqlCreatorAlpha(schema="my_schema", table="my_table")
```
### `select` method
The select method allows you to specify the columns that you want to select:

```python3
sql.select("name", "age", "gender")
```
You can chain multiple select calls to add more columns:

```python3
sql.select("name").select("age", "gender")
```
> Read the *JOINS* to comprehend how to use `join` method
### `where` method
The where method allows you to specify conditions that filter the rows returned by the query:

```python3
sql.where("age > 18").and_where("gender = 'male'")
```
You can chain multiple `where` and `and_where` calls to add more conditions:

```python3
sql.where("age > 18").and_where("gender = 'male'").or_where("country = 'RUSSIA'")
```
The where method supports the following operators: =, !=, >, <, >=, <=, LIKE, NOT LIKE, IN, NOT IN, IS NULL, IS NOT NULL.

### `group_by` method
The group_by method allows you to group the rows returned by the query based on one or more columns:

```python3
sql.group_by("gender")
```
You can pass more parameters to add more columns:

```python3
sql.group_by("gender", "age")
```
### `insert` method
The insert method allows you to insert a new row into the table:

```python3
sql.insert({"name": "Alice", "age": 25, "gender": "female"})
```
Or use keywords:
```python3
sql.insert(name="Alice", age=25, gender="female")
```

### `set` method
The update method allows you to update one or more rows in the table:

```python3
sql.set({"name": "Alice", "age": 25}).where("id = 1")
```
The set method also allows you to use keywords values for the columns:

```python3
sql.set(name="Alice", age=25)
```

### `with_as` method
Use the `with_as` method to add the CTE to the query:

```python3
cte1 = SqlCreatorAlpha(table='table')
cte1.select('1', '2')
sql.select().with_as(name='table_1', sql=cte1)
```
You can also add multiple CTEs using keyword arguments:
```python3
sql.select().with_as(
    table_1=cte1, 
    table_2=cte2
)
```
For this example it will return:

```sql
WITH table_1 AS (
    SELECT region, SUM(amount) AS total_sales
    FROM orders
    GROUP BY region
), table_2 AS (
    SELECT region
    FROM regional_sales
    WHERE total_sales > 2
)
SELECT region,
       product,
       SUM(quantity) AS product_units,
       SUM(amount) AS product_sales
FROM orders
WHERE region IN (SELECT region FROM top_regions)
GROUP BY region, product;
```
### **Your first `.select()` will be added to the end**
### `delete` method
The delete method allows you to delete one or more rows from the table:

```python3
sql.delete().where("age < 18")
```

### `returning` method
The returning method allows you to add a returning value(s) to the query:

```python3
sql.insert(value=1).returning(True)
```

### `clear` method
If you want to clear the query without re-initing your class, you can just use `.clear` method.

### `__build__` method
The build method 
New `Executor` knows about this class and you should not specify query or worry about your queries.
It uses the hidden `__build__` method to cast sql to SqlAlchemy 2.x and psycopg2/3

## Errors

### `ignore_errors: bool`
This parameter is important. If you'll use any of `delete`, `set`, `insert` methods **without** `where` 
it will raise an error `WhereError`. To turn off this exception you can set
```python3
sql.ignore_errors = True
```
 
## JOINS
You can now use the `join()`, `left_join()`, `right_join()`, and `full_join()` methods
to add join clauses to your SQL query. 
The `join()` method takes the name of the table to join with,
a condition to join on, and an optional join type (defaulting to "INNER").
The `left_join()`, `right_join()`, and `full_join()` methods are convenience methods that call `join()`
with the appropriate join type.

Here's an example of how to use the `join()` method:
```python3
sql = SqlCreatorAlpha(schema="my_schema", table="my_table")
sql.select("my_table.name", "orders.order_date").join(
    "orders", "my_table.id = orders.customer_id"
)
>>> SELECT my_table.name, orders.order_date FROM my_schema.my_table INNER JOIN my_schema.orders ON my_table.id = orders.customer_id
```
This example builds a SQL select query to retrieve the name column from my_table and the order_date column from orders,
joined on the id column in my_table and the customer_id column in orders. Note the use of the `join()` method
to add the join clause to the query.