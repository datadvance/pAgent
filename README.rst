pAgent
======

pAgent is crossplatform local job manager and HTTP/Websocket proxy. It uses
`pRpc <https://github.com/datadvance/pRpc>`_
to expose a rich API allowing to run multiple jobs, provide the proxy access to
their listen ports and to upload and download files.

It is intended to be used together with `pRouter <https://github.com/datadvance/pRouter>`_
as a part of a larger software system that requires executing distributed interactive jobs.

Configuration
-------------

pAgent can be configured from both command line and from YAML config file.

Command line description::

    pAgent - remote process manager and reverse proxy.

    optional arguments:
    -h, --help            show this help message and exit
    --config CONFIG       config file
    --log-level {debug,info,fatal,error,warning}
                            output log level
    --connection-debug    enable additional debug output for RPC connections
    --set SET             set config parameter, format is 'config_key=value',
                            values are interpreted as python literals, may appear
                            multiple times

    Config parameters:

        client.address - Master server remote address. Format is "host:port", without any schema or path.
        client.enabled - Enable client mode - attempts to connect to a fixed remote peer as master server.
        client.exit_on_fail - Immediately terminate agent if connection to master server fails.
        client.reconnect_delay - Delay between reconnect attempts.
        client.token - Authentication token.

        control.enabled - Enable (optional) administration API. Control API has no authorization and should not be available over WAN.
        control.interface - Interface (ip address) to listen on.
        control.port - Port number to use. Can be set to 0 to use any free port.

        identity.name - Human-friendly agent name (optional).
        identity.uid - Agent's unique identifier. Arbitrary non-empty string.

        jobs.background_threads - Number of threads to use for long running operations (e.g. removing the job sandbox).
        jobs.root - Root directory for session sandbox. Defaults to system temp.

        properties - Dictionary of custom properties that are published to local jobs (as environment variables) and to remote peers (as connection handshake).

        server.accept_tokens - List of valid client tokens.
        server.enabled - Enable agent server (passive) mode.
        server.interface - Interface (ip address) to listen on.
        server.port - Port number to use. Can be set to 0 to use any free port.

Contributing
------------

This project is developed and maintained by DATADVANCE LLC. Please
submit an issue if you have any questions or want to suggest an
improvement.

Acknowledgements
----------------

This work is supported by the Russian Foundation for Basic Research
(project No. 15-29-07043).
