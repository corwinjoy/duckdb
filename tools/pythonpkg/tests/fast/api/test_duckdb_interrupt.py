# Test that long running queries properly respond to interrupts
# As reported in https://github.com/duckdb/duckdb/issues/10216
# This is important for interactive responses like in Jupyter notebook
# Adapt interrupt test from ipython as shown here:
# https://github.com/itamarst/ipython/blob/561d08809d08a4473e5a085392678544888fdb61/IPython/utils/tests/test_process.py

def print_config():
    import sys
    print("Python version:")
    print(sys.version)
    print("Python paths:")
    for p in sys.path:
        print(p)

print_config()
print("loading libraries")

# import pytest
import duckdb
import pandas as pd
import numpy as np
import signal
import time
from _thread import interrupt_main  # Py 3
import threading

print("loading libraries complete")



def large_test_data_gen():
    print("generating test data")
    n = 1_000_000
    columns = 100
    df = pd.DataFrame({f'col{i}': 1000 * np.random.sample(n) for i in range(columns)})
    print("test data generation complete")
    return df

def long_running_query():
    return """
    SELECT 
    DISTINCT ON (floor(col0))
    *
    FROM df
    ORDER by col0 DESC
    LIMIT 5                 
    """

def limited_column_query():
    return """
    SELECT 
    DISTINCT ON (floor(col0))
    col0, col1, col2
    FROM df
    ORDER by col0 DESC
    LIMIT 5                 
    """


class TestQueryInterrupt(object):
    def configure_duckdb(self):
        # Configure memory and threads for test
        duckdb.execute(
            """
            -- set the memory limit of the system 
            SET memory_limit = '5000GB';
            -- configure the system to use x threads
            SET threads TO 1;
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

        print(f"testing {query_name}")
        result = None
        print("preparing query")
        qry = query()
        df = test_data
        stmt = duckdb.sql(qry)
        print("query run")
        threading.Thread().start()
        start = time.time()
        try:
            print("fetching results")
            result = stmt.fetchall()
            print("results fetched")
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
        print("elapsed time: " + str(elapsed))
        # assert elapsed < 2, query_name + "Query didn't respond to interrupt fast enough. Took %s seconds to respond." % (end - start)
    
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
        

def run_test():
    print("Starting test.")
    mytest = TestQueryInterrupt()
    mytest.test_system_interrupt()
    print("Tests complete!")


run_test()
