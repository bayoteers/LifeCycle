"""PyTables cache

"""

import logging

from tables import *

range = xrange


class Cache(object):
    """Responsible for managing dumps from Bugzilla and the rebuilt history.
    
    """
    def __init__(self, config):
        """
        
        """
        self.config = config
        self.mode = self.get_mode(config.run.start_at)
        file = openFile(config.workdir.cache_file, mode=self.mode,
            title='Cache')
        logging.info('Using cache file: %s', config.workdir.cache_file)
        self.file = file
        root = self.root = file.root
        # XXX: move to dump
        if not 'dumps' in root:
            dumps = file.createGroup('/', 'dumps', 'Database dumps')
        else:
            dumps = root.dumps
        self.dumps = dumps
        
    @staticmethod
    def get_mode(start_at):
        """Choose the most conservative file mode based on start stage.
        
        """
        if start_at == 'count':
            mode = 'r'
        elif start_at == 'rebuild':
            mode = 'a'
        elif start_at == 'dump':
            mode = 'w'
        return mode
    
    def make_table(self, cols, name, context):
        """Make a "dumb" table to save dumps in, i.e. a table with every
        column with the same varchar(255) (in SQL terms) type. Performance 
        could be improved by inspecting the SQL table definition and using 
        corresponding HDFS5 types, but it's fast now anyway.
        
        """
        result = {}
        for i in range(0, len(cols)):
            result[cols[i]] = StringCol(255, pos=i)
        table = type(name, (IsDescription,), result)
        return self.file.createTable(context, name, table)
        
        
class Coord(IsDescription):
    """PyTables model for rebuilt history coordinates. Comments show the 
    corresponding SQL types used by Bugzilla. Performance could be improved by 
    using EnumCol type for columns with fixed contents.
    
    A slight problem is that timestamps are saved as integers, so they need
    to be converted to datetime objects when in use, and this conversion
    should be automated somehow by using a custom type for this model.
    
    """
    # bugs.bug_id mediumint(9)
    id = IntCol(4, pos=0)
    # bugs.keywords mediumtext
    keywords = StringCol(128, pos=1)
    # bugs.bug_status varchar(64)
    status = StringCol(16, pos=2)
    #    status = EnumCol(Enum(Retriever.STATUS_TYPES), 'creation', base=Int32Atom())
    # bugs.resolution varchar(64)
    resolution = StringCol(64, pos=3)
    # bugs.bug_severity varchar(64)
    severity = StringCol(64, pos=4)
    # bugs.creation_ts datetime
    created = IntCol(8, pos=5)
    # bugs.priority varchar(64)
    priority = StringCol(64, pos=6)
    # classifications.name varchar(64)
    program = StringCol(64, pos=7)
    # products.name varchar(64)
    component = StringCol(64, pos=8)
    # components.name varchar(64)
    subcomponent = StringCol(64, pos=9)
    # bugs.delta_ts datetime
    ts = IntCol(8, pos=10)
    # bugs.creation_ts datetime
    creation_ts = IntCol(8, pos=11)
    # flags info varchar(64)
    flags = StringCol(128, pos=12)
    # changed_fields, stores the field that changed varchar(64)
    changed_fields = StringCol(128, pos=13)
    # removed stores what was removed varchar(64)
    removed = StringCol(256, pos=14)
