from __future__ import with_statement, absolute_import

import time
from contextlib import closing

import psycopg2

from . import print_row_progress, status_logger
from .postgres_writer import PostgresWriter
from .master_search import master_search


class RowLimitError(Exception):
    pass


class PostgresDbWriter(PostgresWriter):
    """Class used to stream DDL and/or data
    from a MySQL server to a PostgreSQL.

    :Parameters:
      - `db_options`: :py:obj:`dict` containing connection specific variables
      - `verbose`: whether or not to log progress to :py:obj:`stdout`

    """
    class FileObjFaker(object):
        """A file-like class to support streaming
        table data directly to :py:meth:`pscopg2.copy_from`.

        :Parameters:
          - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.
          - `data`:
          - `processor`:
          - `verbose`: whether or not to log progress to :py:obj:`stdout`
        """
        def __init__(self, table, data, processor, verbose=False, row_limit=None):
            self.data = iter(data)
            self.table = table
            self.processor = processor
            self.verbose = verbose
            self.row_limit = row_limit
            self.eof_found = False
            self._row_counter = 0

            if verbose:
                self.idx = 1
                self.start_time = time.time()
                self.prev_val_len = 0
                self.prev_idx = 0

        def flush_row_counter(self):
            self._row_counter = 0

        def readline(self, *args, **kwargs):
            try:
                if self.row_limit is not None and self._row_counter >= self.row_limit:
                    raise RowLimitError()
                row = list(self.data.next())
                self._row_counter += 1
            except RowLimitError:
                # fake stop iteration
                if self.verbose:
                    print('')
                return ''
            except StopIteration:
                # real stop iteration
                self.eof_found = True
                if self.verbose:
                    print('')
                return ''
            else:
                self.processor(self.table, row)
                try:
                    return '%s\n' % ('\t'.join(row))
                except UnicodeDecodeError:
                    return '%s\n' % ('\t'.join(r.decode('utf8') for r in row))
            finally:
                if self.verbose:
                    if (self.idx % 20000) == 0:
                        now = time.time()
                        elapsed = now - self.start_time
                        val = '%.2f rows/sec [%s] ' % ((self.idx - self.prev_idx) / elapsed, self.idx)
                        print_row_progress('%s%s' % (("\b" * self.prev_val_len), val)),
                        self.prev_val_len = len(val) + 3
                        self.start_time = now
                        self.prev_idx = self.idx + 0
                    self.idx += 1

        def read(self, *args, **kwargs):
            return self.readline(*args, **kwargs)

    def __init__(self, db_options, verbose=False):
        super(PostgresDbWriter, self).__init__()
        self.verbose = verbose
        self.db_options = {
            'host': db_options['hostname'],
            'port': db_options.get('port', 5432),
            'database': db_options['database'],
            'password': db_options.get('password', None) or '',
            'user': db_options['username'],
            }
        if ':' in db_options['database']:
            self.db_options['database'], self.schema = self.db_options['database'].split(':')
        else:
            self.schema = None
        self.open()

    def open(self):
        if len(self.db_options['host'].split(',')) > 1:
            print('multiple servers found server. looking for master')
            self.db_options['host'] = master_search(self.db_options['host'].split(','),
                                                    self.db_options['port'],
                                                    self.db_options['database'],
                                                    self.db_options['user'],
                                                    self.db_options['password'])
        self.conn = psycopg2.connect(**self.db_options)
        with closing(self.conn.cursor()) as cur:
            if self.schema:
                cur.execute('SET search_path TO %s' % self.schema)
            cur.execute('SET client_encoding = \'UTF8\'')
            if self.conn.server_version >= 80200:
                cur.execute('SET standard_conforming_strings = off')
            cur.execute('SET check_function_bodies = false')
            cur.execute('SET client_min_messages = warning')

    def query(self, sql, args=(), one=False):
        with closing(self.conn.cursor()) as cur:
            cur.execute(sql, args)
            return cur.fetchone() if one else cur

    def execute(self, sql, args=(), many=False):
        with closing(self.conn.cursor()) as cur:
            if many:
                cur.executemany(sql, args)
            else:
                cur.execute(sql, args)
            self.conn.commit()

    def copy_from(self, file_obj, table_name, columns):
        file_obj.row_limit = 1000000
        while not file_obj.eof_found:
            print('table %s: new insert(rowlimit: %s)' % (table_name, file_obj.row_limit))
            with closing(self.conn.cursor()) as cur:
                cur.copy_from(file_obj,
                              table=table_name,
                              columns=columns
                              )
            self.conn.commit()
            file_obj.flush_row_counter()
            # debug message
            if not file_obj.eof_found:
                print('table %s: too many rows. interrupt+commit' % table_name)
            else:
                print('table %s: insert completed' % table_name)

    def close(self):
        """Closes connection to the PostgreSQL server"""
        self.conn.close()

    def exists(self, relname):
        rc = self.query('SELECT COUNT(!) FROM pg_class WHERE relname = %s', (relname, ), one=True)
        return rc and int(rc[0]) == 1

    @status_logger
    def truncate(self, table):
        """Send DDL to truncate the specified `table`

        :Parameters:
          - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.

        Returns None
        """
        truncate_sql, serial_key_sql = super(PostgresDbWriter, self).truncate(table)
        self.execute(truncate_sql)
        if serial_key_sql:
            self.execute(serial_key_sql)

    @status_logger
    def write_table(self, table):
        """Send DDL to create the specified `table`

        :Parameters:
          - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.

        Returns None
        """
        table_sql, serial_key_sql = super(PostgresDbWriter, self).write_table(table)
        for sql in serial_key_sql + table_sql:
            self.execute(sql)

    @status_logger
    def write_indexes(self, table):
        """Send DDL to create the specified `table` indexes

        :Parameters:
          - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.

        Returns None
        """
        index_sql = super(PostgresDbWriter, self).write_indexes(table)
        for sql in index_sql:
            self.execute(sql)

    @status_logger
    def write_constraints(self, table):
        """Send DDL to create the specified `table` constraints

        :Parameters:
          - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.

        Returns None
        """
        constraint_sql = super(PostgresDbWriter, self).write_constraints(table)
        for sql in constraint_sql:
            self.execute(sql)

    @status_logger
    def write_contents(self, table, reader):
        """Write the contents of `table`

        :Parameters:
          - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.
          - `reader`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader` object that allows reading from the data source.

        Returns None
        """
        f = self.FileObjFaker(table, reader.read(table), self.process_row, self.verbose)
        self.copy_from(f, '"%s"' % table.name, ['"%s"' % c['name'] for c in table.columns])
