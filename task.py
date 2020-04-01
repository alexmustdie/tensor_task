import os
import csv
import pathlib
import sqlite3

from datetime import datetime


class Task:

    DB_FILE_NAME = 'tensor_task.db'

    DATA_PATH = 'data/'
    OUTPUT_PATH = 'output/'

    conn = None

    def __init__(self):

        if os.path.exists(self.DB_FILE_NAME):
            os.remove(self.DB_FILE_NAME)

        self.conn = sqlite3.connect(self.DB_FILE_NAME)
        self.cursor = self.conn.cursor()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_tables(self):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, release_date TEXT)')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS workers (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS projects_workers (project_id INTEGER, worker_id INTEGER, is_manager INTEGER, days_spent INTEGER)')

    def load_data(self):

        for path in pathlib.Path(self.DATA_PATH).iterdir():

            if path.is_file():

                with open(path, 'r') as file:

                    data = list(csv.reader(file))

                    worker_names = data[0][3:]
                    worker_ids = []

                    for worker_name in worker_names:
                        self.cursor.execute('SELECT id FROM workers WHERE name = \'%s\'' % worker_name)
                        worker = self.cursor.fetchone()
                        if worker:
                            worker_ids.append(worker[0])
                        else:
                            self.cursor.execute('INSERT INTO workers VALUES (null, \'%s\')' % worker_name)
                            worker_ids.append(self.cursor.lastrowid)

                    for row in data[1:]:

                        self.cursor.execute('INSERT INTO projects VALUES (null, \'%s\', \'%s\')' % (row[0], row[2]))
                        project_id = self.cursor.lastrowid
                        manager_name = row[1]

                        i = 0
                        for worker_id, worker_name in zip(worker_ids, worker_names):
                            self.cursor.execute('INSERT INTO projects_workers VALUES (%d, %d, %d, %d)'
                                                % (project_id, worker_id, int(worker_name == manager_name), int(row[i + 3])))
                            i += 1

                    self.conn.commit()

    def export_data(self):

        self.cursor.execute('SELECT p.title, w.name, p.release_date FROM projects p\
            INNER JOIN projects_workers p_w ON p.id = p_w.project_id\
            INNER JOIN workers w ON p_w.worker_id = w.id\
            WHERE p_w.is_manager = 1\
            ORDER BY p.release_date ASC')

        projects = self.cursor.fetchall()

        if projects:

            now_timestamp = datetime.now().replace(microsecond=0).timestamp()

            with open('%sprojects_%.0f.csv' % (self.OUTPUT_PATH, now_timestamp), 'w') as file:
                writer = csv.writer(file)
                writer.writerow(['Название проекта', 'Руководитель', 'Дата сдачи'])
                [writer.writerow(row) for row in projects]
