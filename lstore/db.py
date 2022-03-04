from lstore.table import Table
from lstore.Bufferpool import Bufferpool
import os
# import stat

# counts the total number of records (base and tail) in the database
total_records = 0
base_records = 0
tail_records = 0

class Database():

    def __init__(self):
        self.tables = {}
        # self.total_records = 0
        # self.base_records = 0
        # self.tail_records = 0
        pass

    # TODO: initialize the database
    def open(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
        dbBufferpool = Bufferpool()
        # os.chmod(path, mode= stat.S_fIRWXO)

        # fileObject = None
        # if not os.path.exists(path):
        #     fileObject = open(path, "x")
        # else:
        #     fileObject = open(path, "r")
        # print(fileObject.name)
        # fileObject.close()

        # read any needed metadata in files
        # total_records = self.total_records 
        # base_records = self.base_records 
        # tail_records = self.tail_records 
        pass

    # TODO: write everything that is on the bufferpool back into the file
    # QUESTIONS: how to write back to file if data is modified?
    # do we modify the existing content or do we append to the end?
    # e.g. if a page range is updated, how does this get written back to the disk?
    def close(self):
        # write database metadata to files
        # self.total_records = total_records
        # self.base_records = base_records 
        # self.tail_records = tail_records 
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        self.tables[name] = table
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name in self.tables:
            self.tables.pop(name)
        else:
            print("Error:", name, "does not exist in the database.")

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables.get(name)
