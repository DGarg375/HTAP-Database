from lstore.page import ENTRY_SIZE
from lstore.LogicalPage import NUM_METADATA_COLUMNS, RID_COLUMN

BUFFERPOOL_SIZE = 4

class Bufferpool:

    def __init__(self):
        # self.file_path = filePath
        # each frame holds a Page Range (the data)
        # -> create a separate file to make this an object (similar to record object that stores all needed info) -> did this
        self.frames = [None] * BUFFERPOOL_SIZE
        # key = (tableName, pagerangeid), value = frame_id
        self.bufferpool_directory = {}
        # key = RID, value = line in file
        self.disk_directory = {}
        pass

    # function to check if bufferpool still has space
    # is this function too convoluted?
    def has_capacity(self):
        # when a page range is evicted, just make it 'None' in the list
        hasEmptyFrame = False
        for frame in self.frames:
            if frame == None:
                hasEmptyFrame = True
        return hasEmptyFrame

    # TODO: figure out parameters and complete function
    # function that writes a page range from the disk to the bufferpool
    # -> used when record's page range not found in bufferpool
    def write_to_bufferpool(self, tableName, pagerangeid):
        frameid = None
        # if bufferpool full, evict an existing frame
        if not self.has_capacity():
            frameid = self.evict_frame() 
            if frameid == None:
                print("Number of pins > 0. All page ranges still have active transactions.")
                return
        # locate the first empty slot
        else:
            for index, frame in enumerate(self.frames):
                if frame == None:
                    frameid = index
                    break

        self.bufferpool_directory[(tableName, pagerangeid)] = frameid
        new_frame = self.read_from_disk(tableName, pagerangeid)
        self.frames[frameid] = new_frame

    # TODO: figure out parameters and complete function
    # function that searches and returns the requested record's page range
    # -> if requested record's page range is not found in bufferpool, call write_to_bufferpool()
    # -> look for record in the page ranges in the bufferpool
    # -> return the Page Range requested
    def read_bufferpool(self, tableName, pagerangeid):
        if (tableName, pagerangeid) not in self.bufferpool_directory.keys():
            self.write_to_bufferpool(tableName, pagerangeid)

        frameid = self.bufferpool_directory[(tableName, pagerangeid)]
        existing_frame = self.frames[frameid]
        existing_frame.increment_access_count()
        existing_frame.increment_pin_count()
        return existing_frame.page_range


    # TODO: figure out parameters and complete function
    def update_frame(self, tableName, pagerangeid, new_page_range):
        frameid = self.bufferpool_directory[(tableName, pagerangeid)]
        existing_frame = self.frames[frameid]
        existing_frame.update_page_range(new_page_range)

    # TODO: figure out parameters and complete function
    # function that decrements the number of pins
    # because need to decrement pin when transaction is done with the frame
    def done_with_frame(self, tableName, pagerangeid):
        frameid = self.bufferpool_directory[(tableName, pagerangeid)]
        existing_frame = self.frames[frameid]
        existing_frame.decrement_pin_count()

    # TODO: figure out parameters and complete function
    # function that evicts a page range when bufferpool is full
    # -> use when record's page range not found in bufferpool and bufferpool has no more capacity
    # -> must write the evicted page range back onto the disk
    # return the frameid of the evicted page range
    def evict_frame(self):
        least_accessed_frameid = 0
        min_num_accesses = self.frames[0]
        for i in range(1, BUFFERPOOL_SIZE):
            if self.frames[i].num_accesses < min_num_accesses:
                least_accessed_frameid = i
                min_num_accesses = self.frames[i].num_accesses
        # must make sure no transactions are still accessing the page range that will be evicted
        frame = self.frames[least_accessed_frameid]
        if frame.num_pins == 0:
            # need to write the frame to disk if it is dirty
            if frame.isDirty == 1:
                # need to check parameters of write_to_disk()
                self.write_to_disk(frame)
            self.frames[least_accessed_frameid] = None
            self.bufferpool_directory.pop((frame.tableName, frame.page_range_id))
            return least_accessed_frameid
        else:
            return None
    
    # TODO: figure out parameters and complete function
    # function that empties the bufferpool and writes everything in the bufferpool back to the file
    # -> used in db.close()
    def empty_bufferpool(self):
        for frame in self.frames:
            self.write_to_disk(frame)

    # TODO: figure out parameters and complete function
    # function that writes a page range from the bufferpool to the disk
    # -> should be used in evict_frame() and empty_bufferpool() functions
    # -> NEED TO CHECK ON THE PARAMETERS
    def write_to_disk(self, frame):
        # get number of base pages in page range
        num_base_pages = len(frame.page_range.base_pages)
        # get number of tail pages in page range
        num_tail_pages = len(frame.page_range.tail_pages)
        # get total number of columns per record
        # size = frame.page_range.num_data_columns + NUM_METADATA_COLUMNS

        # open file and store [pagerangeid, number of base pages, number of tail pages]
        # ranges = open("page_ranges", "w")
        # ranges.write(str(frame.page_range_id))
        # ranges.write(" ")
        # ranges.write(str(num_base_pages))
        # ranges.write(" ")
        # ranges.write(str(num_tail_pages))
        # ranges.write("\n")
        # ranges.close()
        
        # open file to store contents of base pages in bytes in a binary file
        base_pages = open("base_pages.bin", "wb")
        # loop through base pages
        for i in range (num_base_pages):
            # loop through physical pages 
            for j in range (frame.page_range.base_pages[i].physical_pages[0].num_records):
                # loop through each record within a page 
                for k in range (len(frame.page_range.base_pages[i].physical_pages)):
                    self.disk_directory[frame.page_range.base_pages[i].physical_pages[RID_COLUMN].read(j)] = base_pages.tell()
                    startIndex = j * ENTRY_SIZE
                    endIndex = startIndex + ENTRY_SIZE 
                    base_pages.write(frame.page_range.base_pages[i].physical_pages[k].data[startIndex:endIndex])
                    #print(str(frame.page_range.base_pages[i].physical_pages[j].read(k)))
                    #base_pages.write(b'\n')
                    #self.disk_directory[frame.page_range.base_pages[i].physical_pages[RID_COLUMN].read(j)] = base_pages.tell()
        base_pages.close()

        # open file to store contents of tail pages in bytes in a binary file
        if len(frame.page_range.tail_pages) != 0:
            tail_pages = open("tail_pages.bin", "wb")
            for i in range (num_tail_pages):
                for j in range (frame.page_range.tail_pages[i].physical_pages[0].num_records):
                    for k in range (len(frame.page_range.tail_pages[i].physical_pages)):
                        self.disk_directory[frame.page_range.tail_pages[i].physical_pages[RID_COLUMN].read(j)] = tail_pages.tell()
                        startIndex = j * ENTRY_SIZE
                        endIndex = startIndex + ENTRY_SIZE
                        tail_pages.write(frame.page_range.tail_pages[i].physical_pages[k].data[startIndex:endIndex])
                        #tail_pages.write(b'\n')
                        print(str(j) , " " , str(frame.page_range.tail_pages[i].physical_pages[k].read(j)))
                        #self.disk_directory[frame.page_range.tail_pages[i].physical_pages[RID_COLUMN].read(j)] = tail_pages.tell()
            tail_pages.close()
        pass

    # TODO: figure out parameters and complete function
    # function that looks for a specific page range from the disk
    # -> should be used in write_to_bufferpool() function
    # -> return a Frame
    def read_from_disk(self, tablename, pagerangeid):
        pass

    # TODO: figure out parameters and complete function
    # need to store page_directory and indexing information
    # pickle
    def store_table_metadata(self, name, key, num_columns, page_directory, keyToRID, index):
        pass

    # TODO: figure out parameters and complete function
    # need to store the 3 global variables for the database
    # pickle
    def store_db_metadata(self):
        pass

    # TODO: CREATE FUNCTIONS AS NEEDED
