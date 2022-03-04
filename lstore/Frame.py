class Frame:
    def __init__(self, tableName, page_range_id, disk_location_id, page_range):
        self.tableName = tableName
        self.page_range_id = page_range_id
        self.disk_location_id = disk_location_id
        self.num_accesses = 0
        self.num_pins = 0
        self.isDirty = 0
        self.page_range = page_range
        pass

    def increment_access_count(self):
        self.num_accesses += 1

    def increment_pin_count(self):
        self.num_pins += 1
    
    def decrement_pin_count(self):
        self.num_pins -= 1

    def is_dirty(self):
        self.isDirty = 1

    def update_page_range(self, data):
        self.data = data
        self.is_dirty()

