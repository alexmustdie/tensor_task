import task

if __name__ == '__main__':
    try:
        my_task = task.Task()
        my_task.create_tables()
        my_task.load_data()
        my_task.export_data()
    except Exception as e:
        print('Exception: %s' % e)
