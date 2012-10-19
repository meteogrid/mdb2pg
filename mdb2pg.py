"""
Dumps a Microsoft Access MDB file into a postgresql database.

Usage: mdb2pg.py mdbfile [dbname]

Requires mdbtools (http://mdbtools.sourceforge.net)

Copyright (c) 2012 Alberto Valverde Gonzalez <alberto@meteogrid.com>

Permission is hereby granted, free of charge, to any
person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the
Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the
Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice
shall be included in all copies or substantial portions of
the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY
KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import sys
import subprocess
import csv
from StringIO import StringIO

from sqlalchemy import create_engine, MetaData, exc, types

class CommandError(StandardError):
    pass

def getoutput(cmd, input=None):
    p = subprocess.Popen(cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE if input else None)
    stdout, stderr = p.communicate(input)
    if p.returncode==0:
        return stdout
    else:
        raise CommandError("Command '%r' returned %d.\n%s" %
                           (cmd, p.returncode, stderr))
    

class MDB(object):
    def __init__(self, path, ignoreindexes=True):
        self.path = path
        self.ignoreindexes = ignoreindexes

    @property
    def table_names(self):
        return getoutput(['mdb-tables', self.path]).split()

    @property
    def schema(self):
        cmd = ['mdb-schema']
        if self.ignoreindexes:
            cmd.append('--no-indexes')
        cmd.extend([self.path, 'postgres'])
        schema = getoutput(cmd)
        return schema.replace('Postgres_Unknown 0x10', 'CHAR(255)')

    def iter_rows(self, table_name):
        stdout = getoutput(['mdb-export', self.path, table_name])
        buf = iter(StringIO(stdout))
        fieldnames = buf.next().strip().split(',')
        reader = csv.DictReader(buf, fieldnames)
        return reader

def export_to_postgres(mdb, dbname, create=True):
    if create:
        getoutput(['createdb', dbname])
        getoutput(['psql', dbname], mdb.schema)
    engine = create_engine('postgres:///'+dbname)
    meta = MetaData(bind=engine, reflect=True)
    conn = engine.connect()
    for table in meta.sorted_tables:
        print>>sys.stderr, 'Processing %s'%table.name
        rows = list(mdb.iter_rows(table.name))
        if rows:
            for row in rows:
                row = adapt_row(row, table)
                try:
                    conn.execute(table.insert(), row)
                except exc.IntegrityError:
                    print>>sys.stderr, 'IntegrityError on %s: %r'%(
                        table.name, row)
                except exc.DataError:
                    raise
                    print>>sys.stderr, 'DataError on %s: %r'%(
                        table.name, row)
                    

def adapt_row(row, table):
    for k,v in row.items():
        row[k] = adapt_field(v, table.columns[k])
    return row

def adapt_field(value, column):
    if isinstance(column.type, types.Integer):
        if not value:
            return 0
        else:
            return int(value)
    if isinstance(column.type, types.DateTime):
        if not value:
            return None
    return value

def main(argv=sys.argv[1:]):
    if len(argv)<1:
        print>>sys.stderr, "Usage: mdb2pg mdbfile [dbname]"
        return -1
    mdbfile = argv[0]
    if len(argv)>1:
        dbname = argv[1]
    else:
        dbname = mdbfile.split('.')[0]
    mdb = MDB(mdbfile)
    export_to_postgres(mdb, dbname)

if __name__=='__main__':
    sys.exit(main())
