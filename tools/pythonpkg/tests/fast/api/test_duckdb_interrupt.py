import duckdb
import pytest
from duckdb import Value
import pandas as pd
import numpy as np
import signal
import time
from _thread import interrupt_main  # Py 3
import threading

# Test that long running queries properly respond to interrupts
# As reported in https://github.com/duckdb/duckdb/issues/10216
# This is important for interactive responses like in Jupyter notebook
# Adapt interrupt test from ipython as shown here:
# https://github.com/itamarst/ipython/blob/561d08809d08a4473e5a085392678544888fdb61/IPython/utils/tests/test_process.py

def large_test_data_gen():
    n = 1_000_000
    columns = 100
    df = pd.DataFrame({f'col{i}': 1000 * np.random.sample(n) for i in range(columns)})
    return df

def long_running_query(df):
    qry = duckdb.sql("""
    SELECT 
    DISTINCT ON (floor(col0))
    *
    FROM df
    ORDER by col0 DESC
    LIMIT 5                 
    """)
    res = qry.fetchall()
    return res

def limited_column_query(df):
    qry = duckdb.sql("""
    SELECT 
    DISTINCT ON (floor(col0))
    col0, col1, col2
    FROM df
    ORDER by col0 DESC
    LIMIT 5                 
    """)
    res = qry.fetchall()
    return res


class TestQueryInterrupt(object):
    def configure_duckdb(self):
        # Configure memory and threads for test
        duckdb.execute(
            """
            -- set the memory limit of the system to 10GB
            SET memory_limit = '500GB';
            -- configure the system to use 16 threads
            SET threads TO 16;
            -- enable printing of a progress bar during long-running queries
            SET enable_progress_bar = true;
            -- temp dir
            SET temp_directory='.tmp';
            """
        )
        

    def assert_interrupts(self, query_name, query, test_data):
        """
        Interrupt a query after a delay and check response time
        """

        # Some tests can overwrite SIGINT handler (by using pdb for example),
        # which then breaks this test, so just make sure it's operating
        # normally.
        signal.signal(signal.SIGINT, signal.default_int_handler)
        interrupt_delay = 2

        def interrupt():
            # Wait for query to start:
            time.sleep(interrupt_delay)
            interrupt_main(signal.SIGINT)


        result = None
        threading.Thread(target=interrupt).start()
        start = time.time()
        try:
            result = query(test_data)
        except RuntimeError as err:
            # Success!
            assert err.args[0] == 'Query interrupted'
        end = time.time()

        # In case query finished before interrupt, wait for interrupt
        if result:
            try:
                time.sleep(interrupt_delay)
            except KeyboardInterrupt:
                pass
        elapsed = end - start - interrupt_delay
        assert elapsed < 2, query_name + "Query didn't respond to interrupt fast enough. Took %s seconds to respond." % (end - start)
    
    def test_system_interrupt(self):
        """
        Test that queries promptly respond to keyboard interrupts
        """
        self.configure_duckdb()
        large_test_data = large_test_data_gen()
        self.assert_interrupts("limited_column_query", limited_column_query, large_test_data)
        self.assert_interrupts("long_running_query", long_running_query, large_test_data)
        

        """
        # Profile code
        # Not much info since work happens in C

        import cProfile, pstats, io
        from pstats import SortKey
        with cProfile.Profile() as pr:
            # self.assert_interrupts("limited_column_query", limited_column_query, large_test_data)
            self.assert_interrupts("long_running_query", long_running_query, large_test_data)
            s = io.StringIO()
            sortby = SortKey.CUMULATIVE
            ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
            ps.print_stats()
            print(s.getvalue())
        """
        


mytest = TestQueryInterrupt()
mytest.test_system_interrupt()