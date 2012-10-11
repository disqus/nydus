"""
nydus.db.map
~~~~~~~~~~~~

:copyright: (c) 2011 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

from itertools import izip
from collections import defaultdict
from nydus.utils import ThreadPool
from nydus.db.exceptions import CommandError
from nydus.db.promise import EventualCommand, promise_method, change_resolution


class BaseDistributedConnection(object):
    def __init__(self, cluster, workers=None, fail_silently=False):
        self.__cluster = cluster
        self.__workers = min(workers or len(cluster), 16)
        self.__commands = []
        self.__complete = False
        self.__errors = []
        self.__fail_silently = fail_silently
        self.__resolved = False

    def __getattr__(self, attr):
        command = EventualCommand(attr)
        self.__commands.append(command)
        return command

    def __build_pending_commands(self):
        pending_commands = defaultdict(list)

        # build up a list of pending commands and their routing information
        for command in self.__commands:
            if self.__cluster.router:
                name, args, kwargs = command.get_command()
                db_nums = self.__cluster.router.get_dbs(
                    cluster=self.__cluster,
                    attr=name,
                    args=args,
                    kwargs=kwargs,
                )
            else:
                db_nums = self.__cluster.keys()

            for db_num in db_nums:
                # add to pending commands
                pending_commands[db_num].append(command)

        return pending_commands

    @promise_method
    def resolve(self):
        pending_commands = self.__build_pending_commands()

        num_commands = sum(len(v) for v in pending_commands.itervalues())
        if num_commands == 0:
            self.__commands = []

        # Don't bother with the pooling if we only need to do one operation on a single machine
        elif num_commands == 1:
            db_num, (command,) = pending_commands.items()
            self.__commands = [command.resolve(self.__cluster[db_num])]

        else:
            self.execute(pending_commands)

        if not self.__fail_silently and self.__errors:
            raise CommandError(self.__errors)

        self.__resolved = True

    @promise_method
    def execute(self, commands):
        raise NotImplementedError

    def get_results(self):
        assert self.__resolved, 'you must execute the commands before fetching results'

        return self.__commands

    def get_errors(self):
        assert self.__resolved, 'you must execute the commands before fetching results'

        return self.__errors


class DistributedConnection(BaseDistributedConnection):
    @promise_method
    def execute(self, commands):
        # Create the threadpool and pipe jobs into it
        pool = ThreadPool(min(self.__workers, len(commands)))

        # execute our pending commands either in the pool, or using a pipeline
        for db_num, command_list in commands.iteritems():
            for command in command_list:
                # XXX: its important that we clone the command here so we dont override anything
                # in the EventualCommand proxy (it can only resolve once)
                pool.add(hash(command), command.clone().resolve, [self.__cluster[db_num]])

        # Consolidate commands with their appropriate results
        result_map = pool.join()

        for command in self.__commands:
            # we explicitly use the hash as the identifier as that is how it was added to the
            # pool originally
            result = result_map[hash(command)]
            for value in result:
                if isinstance(value, Exception):
                    self.__errors.append((command, value))

            if len(result) == 1:
                result = result[0]

            change_resolution(command, result)


class PipelinedDistributedConnection(BaseDistributedConnection):
    @promise_method
    def execute(self, commands):
        pipes = dict()  # db -> pipeline

        # Create the threadpool and pipe jobs into it
        pool = ThreadPool(min(self.__workers, len(commands)))

        # execute our pending commands either in the pool, or using a pipeline
        for db_num, command_list in commands.iteritems():
            pipes[db_num] = self.__cluster[db_num].get_pipeline()
            for command in command_list:
                # add to pipeline
                pipes[db_num].add(command)

        # We need to finalize our commands with a single execute in pipelines
        for db, pipe in pipes.iteritems():
            pool.add(db, pipe.execute, (), {})

        # Consolidate commands with their appropriate results
        result_map = pool.join()

        # Results get grouped by their command signature, so we have to separate the logic
        for db, result in result_map.iteritems():
            if len(result) == 1:
                result = result[0]

            # Handle internal exception
            if isinstance(result, Exception):
                for command in commands[db]:
                    self.__errors.append((command, result))
                    change_resolution(command, result)

            else:
                for ident, value in izip(commands[db], result):
                    if isinstance(value, Exception):
                        self.__errors.append((command, value))

                    change_resolution(command, value)


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
