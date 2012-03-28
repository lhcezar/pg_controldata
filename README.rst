 WORK IN PROGRESS  
===================

pg_controldata
--------------

Script to extract information from /ControlFileData/ struct (pg_control file).

Usage:

pg_control.py --version_number

Usage:
  pg_control.py [OPTION] [DATADIR]

  Options:
    control-version    show pg_control's versoin number
    catalog-version    show catalog version
    cluster-state      database cluster state (shutdown, starting, in production,
    help               show this help, then exit
    version            output version information, then exit
    last-checkpoint    get lastest checkpoint
    [...]

If no data directory (DATADIR) is specified, the environment variable PGDATA
is used. If there is no PGDATA set, exit quietly
