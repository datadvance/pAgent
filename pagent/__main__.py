#
# coding: utf-8
# Copyright (c) 2017 DATADVANCE
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

import asyncio
import concurrent
import logging
import signal
import sys

import prpc
import yarl

import pagent.agent_app
import pagent.agent_service
import pagent.client
import pagent.config
import pagent.connection_manager
import pagent.control_app
import pagent.jobs
import pagent.job_app
import pagent.identity

LOCALHOST_IP = '127.0.0.1'

LOGGER_NAME_ROOT = 'pagent'
LOGGER_NAME_AGENT_APP = '.'.join((LOGGER_NAME_ROOT, 'AgentApplication'))
LOGGER_NAME_AGENT_SERVER = '.'.join((LOGGER_NAME_ROOT, 'AgentServer'))
LOGGER_NAME_CONTROL_APP = '.'.join((LOGGER_NAME_ROOT, 'ControlApplication'))
LOGGER_NAME_CONTROL_SERVER = '.'.join((LOGGER_NAME_ROOT, 'ControlServer'))
LOGGER_NAME_JOB_APP = '.'.join((LOGGER_NAME_ROOT, 'JobApp'))
LOGGER_NAME_JOB_SERVER = '.'.join((LOGGER_NAME_ROOT, 'JobServer'))


def setup_logging(args):
    """Configure the logging facilities.
    """
    root_logger = logging.getLogger()
    # root logger accepts all messages, filter on handlers level
    root_logger.setLevel(logging.DEBUG)
    for handler in [logging.StreamHandler(sys.stdout)]:
        handler.setFormatter(
            logging.Formatter(
                '[%(process)d] [%(asctime)s] '
                '[%(name)s] [%(levelname)s] %(message)s'
            )
        )
        handler.setLevel(args.log_level.upper())
        root_logger.addHandler(handler)


class ExitHandler(object):
    """Manage exit process - close all long-running tasks.

    Grouped into a class to avoid using globals
    (self._exit_task would be global so main can wait for it).
    """
    def __init__(self, servers, clients, conn_manager, logger, loop):
        self._servers = servers
        self._clients = clients
        self._conn_manager = conn_manager
        self._log = logger
        self._loop = loop
        self._exit_task = None
        self._exit_sent = False
        self._exit_code = 0

    def __call__(self, exit_code=0):
        """Submit exit task to the event loop. Ignores multiple calls.
        """
        if self._exit_sent:
            self._log.debug('Exit in progress, exit request ignored')
            return
        self._log.info('Exit request received')
        self._exit_task = self._loop.create_task(self._loop_exit())
        self._exit_sent = True
        self._exit_code = exit_code

    async def wait(self):
        """If exit task is active, wait for it to finish.
        """
        if self._exit_task:
            await self._exit_task
        return self._exit_code

    async def _loop_exit(self):
        """Actual exit/cleanup implementation.
        """
        for client in self._clients:
            await client.stop()
        # Cleanup current connections.
        # It's done before disabling the server, so that
        # asyncio is less unhappy.
        for connection in self._conn_manager.get_connections():
            await connection.close()
        # Shutdown the server(s) so we don't accept new ones.
        for server in reversed(self._servers):
            await server.shutdown()
        # Cleanup any leftovers.
        for connection in self._conn_manager.get_connections():
            await connection.close()
        # Job manager will be closed in the 'run' function)


def make_signal_handler(exit_handler, loop):
    """Wrap app exit handler as a signal handler.
    """
    def on_exit(signo, frame):
        """Signal handler."""
        # Surprisingly enough, threadsafe loop API is not only 'safe'
        # but also wakes up the loop immediately (so you don't wait
        # for timeout on select/epoll/whatever).
        #
        # Note:
        #   As about threadsafety, it shouldn't be important.
        #   Python docs state that python signal handlers are called
        #   in the main thread anyway (even if e.g. Windows calls
        #   native handlers in a separate thread).
        loop.call_soon_threadsafe(exit_handler)
    return on_exit


def configure_loop(config, main_log):
    """Return the configured asyncio loop.
    """
    loop = asyncio.get_event_loop()
    thread_count = config['jobs']['background_threads']
    main_log.debug('Using %d background threads', thread_count)
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=thread_count,
        thread_name_prefix='background-'
    )
    loop.set_default_executor(executor)
    return loop


async def run(args, config, main_log, loop):
    """Application bootstrap.
    """
    job_manager = pagent.jobs.JobManager(
        workdir_root=config['jobs']['root'],
        properties=config['properties'],
        loop=loop
    )

    async with job_manager:
        clients = []
        servers = []
        lifetime_tasks = []

        identity = pagent.identity.Identity(
            config['identity']['uid'],
            config['identity']['name'],
            config['server']['accept_tokens'],
            config['client']['token'],
            config['properties']
        )

        agent_service = pagent.agent_service.AgentService(job_manager)

        conn_manager = pagent.connection_manager.ConnectionManager(
            debug=args.connection_debug
        )
        # Drop all jobs spawned by a particular connection on connection loss.
        #
        # TODO: obviously, needs to be undone if reconnect support is required.
        conn_manager.on_connection_lost.append(
            job_manager.job_remove_all_by_connection
        )

        if config['client']['enabled'] and not config['client']['address']:
            main_log.warning(
                'Client mode is enabled in configuration, '
                'but remote address is not set. '
                'Client mode disabled.'
            )
            config['client']['enabled'] = False

        # Note the nice circular dependency - control app needs the
        # exit handler, which will actually shutdown the control app.
        #
        # All hail the mutable 'servers' collection
        # that allows us to unroll this.
        exit_handler = ExitHandler(
            servers, clients, conn_manager, main_log, loop
        )

        client = pagent.client.Client(
            agent_service, identity, conn_manager,
            config['client']['address'],
            config['client']['reconnect_delay'],
            config['client']['exit_on_fail'],
            exit_handler,
            loop=loop
        )
        clients.append(client)

        agent_app = pagent.agent_app.get_application(
            agent_service, conn_manager,
            identity, logging.getLogger(LOGGER_NAME_AGENT_APP)
        )
        control_app = pagent.control_app.get_application(
            job_manager, conn_manager,
            identity, exit_handler, logging.getLogger(LOGGER_NAME_CONTROL_APP)
        )
        job_app = pagent.job_app.get_application(
            job_manager, logging.getLogger(LOGGER_NAME_JOB_APP)
        )

        # Job server must be started before server/client modes
        # so we don't have a race between incoming jobs
        # and 'set_job_start_endpoint'.
        job_server = prpc.platform.ws_aiohttp.AsyncServer(
            job_app,
            endpoints=((LOCALHOST_IP, 0),),
            logger=logging.getLogger(LOGGER_NAME_JOB_SERVER)
        )
        (_, job_server_port), = await job_server.start()
        servers.append(job_server)
        lifetime_tasks.append(job_server.wait())

        job_manager.set_job_start_endpoint(
            str(
                yarl.URL.build(
                    scheme='http', host=LOCALHOST_IP, port=job_server_port,
                    path=pagent.job_app.ROUTE_JOB_STARTED
                )
            )
        )

        if config['server']['enabled']:
            main_log.info('Server mode enabled')
            agent_server = prpc.platform.ws_aiohttp.AsyncServer(
                agent_app,
                endpoints=(
                    (config['server']['interface'], config['server']['port']),
                ),
                logger=logging.getLogger(LOGGER_NAME_AGENT_SERVER)
            )
            await agent_server.start()
            servers.append(agent_server)
            lifetime_tasks.append(agent_server.wait())

        if config['control']['enabled']:
            main_log.info('Control API enabled')
            control_server = prpc.platform.ws_aiohttp.AsyncServer(
                control_app,
                endpoints=(
                    (config['control']['interface'], config['control']['port']),
                ),
                logger=logging.getLogger(LOGGER_NAME_CONTROL_SERVER)
            )
            await control_server.start()
            servers.append(control_server)
            lifetime_tasks.append(control_server.wait())

        if config['client']['enabled']:
            main_log.info(
                'Client enabled (remote address: %s)',
                config['client']['address']
            )
            await client.start()

        signal.signal(
            signal.SIGINT,
            make_signal_handler(
                exit_handler, loop
            )
        )
        await asyncio.wait(lifetime_tasks, loop=loop)
        return await exit_handler.wait()


def main():
    """Entry point.
    """
    args, config = pagent.config.initialize()
    setup_logging(args)
    pagent.config.validate(config)

    main_log = logging.getLogger(LOGGER_NAME_ROOT)
    main_log.info('Staring pAgent')

    loop = configure_loop(config, main_log)
    try:
        exit_code = loop.run_until_complete(run(args, config, main_log, loop))
        sys.exit(exit_code)
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


if __name__ == '__main__':
    main()
