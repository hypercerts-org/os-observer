import sqlglot as sql
from oso_dagster.cbt.utils import replace_source_tables, is_same_sql
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.normalize import normalize


def test_replace_table():
    replace = replace_source_tables(sql.to_table("test"), sql.to_table("replacement"))
    result1 = sql.parse_one("select * from test").transform(replace)
    assert sql.parse_one("select * from replacement") == result1

    result2 = sql.parse_one("select * from test as t").transform(replace)
    assert sql.parse_one("select * from replacement as t") == result2

    result3 = sql.parse_one(
        "select * from noreplace as nr inner join test as t on t.t_id = nr.nr_id"
    ).transform(replace)
    expected = sql.parse_one(
        "select * from noreplace as nr inner join replacement as t on t.t_id = nr.nr_id"
    )
    assert is_same_sql(normalize(qualify(result3)), normalize(qualify(expected)))
