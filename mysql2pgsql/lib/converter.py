from __future__ import absolute_import

import enum
from . import print_start_table


class ContinuePoints(enum.IntEnum):

    CREATING_TABLES = 0
    TRUNCATING_TABLES = 1
    WRITING_TABLE_DATA = 2
    CREATING_INDEXES_AND_CONSTRAINTS = 3


class Converter(object):
    def __init__(self, reader, writer, file_options, verbose=False):
        self.verbose = verbose
        self.reader = reader
        self.writer = writer
        self.file_options = file_options
        self.exclude_tables = file_options.get('exclude_tables', [])
        self.only_tables = file_options.get('only_tables', [])
        self.supress_ddl = file_options.get('supress_ddl', None)
        self.supress_data = file_options.get('supress_data', None)
        self.force_truncate = file_options.get('force_truncate', None)

    def convert(self, continue_point=None):

        if continue_point is None:
            continue_point = ['CREATING_TABLES', None]
        continue_label = getattr(ContinuePoints, continue_point[0])
        continue_arg = continue_point[1]

        if self.verbose:
            print_start_table('>>>>>>>>>> STARTING <<<<<<<<<<\n\n')

        tables = [t for t in (t for t in self.reader.tables if t.name not in self.exclude_tables) if not self.only_tables or t.name in self.only_tables]
        if self.only_tables:
            tables.sort(key=lambda t: self.only_tables.index(t.name))
        
        if not self.supress_ddl and continue_label <= ContinuePoints.CREATING_TABLES:
            if self.verbose:
                print_start_table('START CREATING_TABLES')

            for table in tables:
                self.writer.write_table(table)

            if self.verbose:
                print_start_table('DONE CREATING_TABLES')

        if self.force_truncate and self.supress_ddl and continue_label <= ContinuePoints.TRUNCATING_TABLES:
            if self.verbose:
                print_start_table('START TRUNCATING_TABLES')

            for table in tables:
                self.writer.truncate(table)

            if self.verbose:
                print_start_table('DONE TRUNCATING_TABLES')

        if not self.supress_data and continue_label <= ContinuePoints.WRITING_TABLE_DATA:
            if self.verbose:
                print_start_table('START WRITING_TABLE_DATA')

            if continue_arg is not None:
                found = False
                for table in tables:
                    if continue_arg == table:
                        found = True
                    if found:
                        self.writer.write_contents(table, self.reader)
            else:
                for table in tables:
                    self.writer.write_contents(table, self.reader)

            if self.verbose:
                print_start_table('DONE WRITING_TABLE_DATA')

        if not self.supress_ddl and continue_label <= ContinuePoints.CREATING_INDEXES_AND_CONSTRAINTS:
            if self.verbose:
                print_start_table('START CREATING INDEXES_AND_CONSTRAINTS')

            for table in tables:
                self.writer.write_indexes(table)

            for table in tables:
                self.writer.write_constraints(table)

            if self.verbose:
                print_start_table('DONE CREATING INDEXES_AND_CONSTRAINTS')

        if self.verbose:
            print_start_table('\n\n>>>>>>>>>> FINISHED <<<<<<<<<<')

        self.writer.close()
