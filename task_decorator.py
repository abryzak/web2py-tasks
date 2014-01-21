import datetime

NEVER_RUN = datetime.datetime(1970, 1, 1, 0, 0)

class _Task(object):
    def __init__(self, f, name=None, seconds_between_runs=60):
        import inspect
        self.f = f
        self.name = name or f.__name__
        self.seconds_between_runs = seconds_between_runs
        self.arg_names = inspect.getargspec(f).args

    def _select_task(self, create=False):
        from gluon import current
        db = current.db
        task = db(db.task.name == self.name).select().first()
        if create and task is None:
            task = db.task(db.task.insert(name=self.name, last_run=NEVER_RUN))
        return task

    def will_run(self, task=None):
        import datetime
        from gluon import current
        request = current.request
        if not task:
            task = self._select_task()
        last_run = task.last_run if task and task.last_run else NEVER_RUN
        return last_run + datetime.timedelta(seconds=self.seconds_between_runs) <= request.now

    def __call__(self, *args, **kwargs):
        from gluon import current
        db = current.db
        request = current.request
        task = self._select_task(True)
        if not self.will_run(task) and kwargs.get('force', False) != True:
            return None
        result = self.call_original(task)
        db.commit()
        return result

    def call_original(self, task=None):
        from gluon import current
        request = current.request
        if task is None:
            task = self._select_task(True)
        args = []
        for arg_name in self.arg_names:
            if arg_name == 'task':
                args.append(task)
            else:
                args.append(getattr(current, arg_name))
        result = self.f(*args)
        task_dict = task.as_dict()
        task_dict['last_run'] = request.now
        task.update_record(**task_dict)
        return result

def task(tasks=None, **kwargs):
    import inspect
    import datetime
    def wrap(f):
        from gluon import current
        t = _Task(f, **kwargs)
        if tasks is not None: tasks.append(t)
        return t
    return wrap

def define_table(db, *extra_fields):
    from gluon.dal import Field
    args = list(extra_fields)
    args.append(Field('name', notnull=True, unique=True))
    args.append(Field('last_run', 'datetime'))
    db.define_table('task', *args)
