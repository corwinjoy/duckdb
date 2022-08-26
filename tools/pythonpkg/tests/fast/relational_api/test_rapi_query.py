import duckdb
import pytest

@pytest.fixture()
def tbl_table():
    con = duckdb.default_connection
    con.execute("create table tbl (i integer)")
    yield
    con.execute('drop table tbl')

class TestRAPIQuery(object):

    @pytest.mark.parametrize('steps', [1, 2, 3, 4])
    def test_query_chain(self, steps):
        con = duckdb.default_connection
        amount = int(1000000)
        rel = None
        for _ in range(steps):
            rel = con.query(f"select i from range({amount}::BIGINT) tbl(i)")
            amount = amount / 10
            rel = rel.query("rel", f"select * from rel limit {amount}")
        result = rel.execute()
        assert(len(result.fetchall()) == amount)

    @pytest.mark.parametrize('input', [[5,4,3],[], [1000]])
    def test_query_table(self, tbl_table, input):
        con = duckdb.default_connection
        rel = con.table("tbl")
        for row in input:
            rel.insert([row])
        # Querying a table relation
        rel = rel.query("x", "select * from x")
        result = rel.execute()
        assert(result.fetchall() == [tuple([x]) for x in input])

    def test_query_table_unrelated(self, tbl_table):
        con = duckdb.default_connection
        rel = con.table("tbl")
        # Querying a table relation
        rel = rel.query("x", "select 5")
        result = rel.execute()
        assert(result.fetchall() == [(5,)])

    def test_query_broken(self, tbl_table):
        con = duckdb.default_connection
        rel = con.query("select i from range(10000) tbl(i)")
        rel = rel.query("x", "insert into tbl VALUES(5)")
        result = rel.execute()
        # The query has no result, so the original relation wasn't executed in the last `rel.query` call
        assert(len(result.fetchall()) == 10000)
