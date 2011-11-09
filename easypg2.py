import time
import thread
import threading
import psycopg2
import psycopg2.extensions

class Connection(psycopg2.extensions.connection):
    """Just psycopg2 connection with extra functionalities."""

    def __init__(self, dsn, async=0):
        """Constructor."""
        super(Connection, self).__init__(dsn, async)
        self.timestamp = time.time()

    def fetchone(self, operation, *args, **kwargs):
        """Just transacted cursor.execute() and cursor.fetchone()."""
        try:
            cursor = self.cursor()
            cursor.execute(operation, *args, **kwargs)
            return cursor.fetchone()
        finally:
            try:
                cursor.close()
            except:
                pass
            try:
                self.rollback()
            except:
                pass

    def fetchall(self, operation, *args, **kwargs):
        """Just transacted cursor.execute() and cursor.fetchall()."""
        try:
            cursor = self.cursor()
            cursor.execute(operation, *args, **kwargs)
            return cursor.fetchall()
        except:
            raise
        finally:
            try:
                cursor.close()
            except:
                pass
            try:
                self.rollback()
            except:
                pass

    def execute(self, operation, *args, **kwargs):
        """Just transacted cursor.execute()."""
        try:
            cursor = self.cursor()
            cursor.execute(operation, *args, **kwargs)
            self.commit()
            return cursor.rowcount
        finally:
            try:
                cursor.close()
            except:
                pass
            try:
                self.rollback()
            except:
                pass

class ConnectionManager(object): 
    """Manages the connection of each thread."""

    def __init__(self, dsn, timeout):
        """Constructor."""
        self.dsn = dsn
        self.timeout = timeout
        self.conns = {}

    def get_conn(self):
        """Returns the connection of the requesting thread."""
        # Sort out the key and connection/transaction status.
        key = thread.get_ident()
        if key in self.conns:
            if not self.conns[key].closed:
                if self.conns[key].status != psycopg2.extensions.STATUS_READY:
                    # This exception should result in changes in client program.
                    raise Exception("The connection status is not ready!")
                if self.conns[key].get_transaction_status() != psycopg2.extensions.TRANSACTION_STATUS_IDLE:
                    # This exception should result in changes in client program.
                    raise Exception("The connection transaction is not idle!")
            if self.conns[key].closed or time.time() - self.conns[key].timestamp > timeout:
                conn = self.conns.pop(key)
                try:
                    conn.close()
                except:
                    pass
        # Remove left-over, unused connections of perhaps dead threads.
        ids = [t.ident for t in threading.enumerate()]
        for key in self.conns.keys():
            if key not in ids:
                conn = self.conns.pop(key)
                try:
                    conn.close()
                except:
                    pass
        # Set and return the preferred connection.
        return self.conns.setdefault(key, psycopg2.connect(self.dsn, connection_factory=Connection)) 

__director__ = {}
def connect(dsn, timeout=3600):
    """Convenient factory to create/get the connection of each thread, e.g.
    
    import easypg2
    conn = easypg2.connect("dbname=test user=user password=password")
    print conn.fetchall("select * from table_x")
    print conn.fetchall("select * from table_y")
    conn.execute("update table_x set column_a = %s", (value_for_a,))
    conn.close()
    """
    global __director__
    return __director__.setdefault(dsn, ConnectionManager(dsn, timeout)).get_conn()