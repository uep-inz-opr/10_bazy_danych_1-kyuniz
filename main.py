import sqlite3
import csv


class DurationCalculator:

    def __init__(self) -> None:
        self.sqlite_con = sqlite_con = sqlite3.connect(":memory:",
                                                       detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self.cur = sqlite_con.cursor()

    def create_table(self):
        self.cur.execute('''CREATE TABLE polaczenia (from_subscriber data_type INTEGER, 
                          to_subscriber data_type INTEGER, 
                          datetime data_type timestamp, 
                          duration data_type INTEGER , 
                          celltower data_type INTEGER);''')

    def insert_csv(self):
        with open('polaczenia_duze.csv', 'r') as fin:
            # csv.DictReader uses first line in file for column headings by default
            reader = csv.reader(fin, delimiter=";")  # comma is default delimiter
            next(reader, None)  # skip the headers
            rows = [x for x in reader]
            self.cur.executemany(
                "INSERT INTO polaczenia (from_subscriber, to_subscriber, datetime, duration, celltower) VALUES (?, ?, ?, ?, ?);",
                rows)
            self.sqlite_con.commit()

    def calculate_duration_sum(self):
        self.cur.execute('SELECT duration FROM polaczenia')
        val = self.cur.fetchall()
        sum = 0
        for element in val:
            for element2 in element:
                sum += element2
        return sum


duration_calculator = DurationCalculator
duration_calculator.create_table()
duration_calculator.insert_csv()
print(duration_calculator.calculate_duration_sum())
