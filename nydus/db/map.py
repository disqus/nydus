"""
nydus.db.map
~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from collections import defaultdict
from nydus.utils import ThreadPool
from nydus.db.exceptions import CommandError
from nydus.db.promise import EventualCommand, change_resolution


class BaseDistributedConnection(object):
    def __init__(self, cluster, workers=None, fail_silently=False):
        self._commands = []
        self._complete = False
        self._errors = []
        self._resolved = False
        self._cluster = cluster
        self._fail_silently = fail_silently
        self._workers = min(workers or len(cluster), 16)

    def __getattr__(self, attr):
        command = EventualCommand(attr)
        self._commands.append(command)
        return command

    def _build_pending_commands(self):
        pending_commands = defaultdict(list)

        # build up a list of pending commands and their routing information
        for command in self._commands:
            if not command.was_called():
                continue

            if self._cluster.router:
                name, args, kwargs = command.get_command()
                db_nums = self._cluster.router.get_dbs(
                    cluster=self._cluster,
                    attr=name,
                    args=args,
                    kwargs=kwargs,
                )
            else:
                db_nums = self._cluster.keys()

            for db_num in db_nums:
                # add to pending commands
                pending_commands[db_num].append(command)

        return pending_commands

    def get_pool(self, commands):
        return ThreadPool(min(self._workers, len(commands)))

    def resolve(self):
        pending_commands = self._build_pending_commands()

        num_commands = sum(len(v) for v in pending_commands.itervalues())
        # Don't bother with the pooling if we only need to do one operation on a single machine
        if num_commands == 1:
            db_num, (command,) = pending_commands.items()[0]
            self._commands = [command.resolve(self._cluster[db_num])]

        elif num_commands > 1:
            results = self.execute(self._cluster, pending_commands)

            for command in self._commands:
                result = results.get(command)

                if result:
                    for value in result:
                        if isinstance(value, Exception):
                            self._errors.append((command.get_name(), value))

                    # XXX: single path routing (implicit) doesnt return a list
                    if len(result) == 1:
                        result = result[0]

                change_resolution(command, result)

        self._resolved = True

        if not self._fail_silently and self._errors:
            raise CommandError(self._errors)

    def execute(self, cluster, commands):
        """
        Execute the given commands on the cluster.

        The result should be a dictionary mapping the original command to the
        result value.
        """
        raise NotImplementedError

    def get_results(self):
        """
        Returns a list of results (once commands have been resolved).
        """
        assert self._resolved, 'you must execute the commands before fetching results'

        return self._commands

    def get_errors(self):
        """
        Returns a list of errors (once commands have been resolved).
        """
        assert self._resolved, 'you must execute the commands before fetching results'

        return self._errors


class DistributedConnection(BaseDistributedConnection):
    """
    Runs all commands using a simple thread pool, queueing up each command for each database
    it needs to run on.
    """
    def execute(self, cluster, commands):
        # Create the threadpool and pipe jobs into it
        pool = self.get_pool(commands)

        # execute our pending commands either in the pool, or using a pipeline
        for db_num, command_list in commands.iteritems():
            for command in command_list:
                # XXX: its important that we clone the command here so we dont override anything
                # in the EventualCommand proxy (it can only resolve once)
                pool.add(command, command.clone().resolve, [cluster[db_num]])

        return dict(pool.join())


class PipelinedDistributedConnection(BaseDistributedConnection):
    """
    Runs all commands using pipelines, which will execute a single pipe.execute() call
    within a thread pool.
    """
    def execute(self, cluster, commands):
        # db_num: pipeline object
        pipes = {}

        # Create the threadpool and pipe jobs into it
        pool = self.get_pool(commands)

        # execute our pending commands either in the pool, or using a pipeline
        for db_num, command_list in commands.iteritems():
            pipes[db_num] = cluster[db_num].get_pipeline()
            for command in command_list:
                # add to pipeline
                pipes[db_num].add(command.clone())

        # We need to finalize our commands with a single execute in pipelines
        for db_num, pipe in pipes.iteritems():
            pool.add(db_num, pipe.execute, (), {})

        # Consolidate commands with their appropriate results
        db_result_map = pool.join()

        # Results get grouped by their command signature, so we have to separate the logic
        results = defaultdict(list)

        for db_num, db_results in db_result_map.iteritems():
            # Pipelines always execute on a single database
            assert len(db_results) == 1
            db_results = db_results[0]

            # if pipe.execute (within nydus) fails, this will be an exception object
            if isinstance(db_results, Exception):
                for command in commands[db_num]:
                    results[command].append(db_results)
                continue

            for command, result in db_results.iteritems():
                results[command].append(result)

        return results


class DistributedContextManager(object):
    def __init__(self, cluster, workers=None, **kwargs):
        if self.can_pipeline(cluster):
            cls = PipelinedDistributedConnection
        else:
            cls = DistributedConnection
        self.connection = cls(cluster, workers, **kwargs)

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc_value, tb):
        # we need to break up each command and route it
        self.connection.resolve()

    def can_pipeline(self, cluster):
        return all(cluster[n].supports_pipelines for n in cluster)
