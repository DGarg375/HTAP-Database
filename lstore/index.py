from lstore.Bufferpool import Bufferpool
from BTree.OOBTree import OOBTree
from lstore.LogicalPage import LogicalPage

"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""


class Index:

    def __init__(self, table):
        self.table = table
        # One index for each table. All our empty initially.
        # INDEXING IMPLEMENT IN MILESTONE 2
        t = OOBTree()
        self.indices = [None] * table.num_columns
        # self.indices[self.table.key] = t

    """
    # returns the location of all records with the given value on column "column"
    """
    # NOT SURE IF THIS IS IMPLEMENTED IN MILESTONE 1

    def locate(self, column, value):
        t = self.indices[column]
        if t.has_key(value):
            return t[value]
        return None

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    # NOT SURE IF THIS IS IMPLEMENTED IN MILESTONE 1

    def locate_range(self, begin, end, column):
        return list(self.indices[column].values(min=begin, max=end))

    """
    # optional: Create index on specific column
    """
    # IMPLEMENT IN MILESTONE 2

    def create_index(self, column_number):
        t = OOBTree()
        self.indices[column_number] = t
        for i in range(self.physical_pages[column_number].num_records):
            # create indexing on
            # create index for every record in that column.
            # Map index to record's RID in table
            # do we need to update indexes because professor said once RID is assigned for a record it stays the same (and RID maps to LID so we can access updates to records like this: key -> RID -> LID)??
            pass

    """
    # optional: Drop index of specific column
    """
    # IMPLEMENT IN MILESTONE 2

    def drop_index(self, column_number):
        self.indices[column_number] = None
