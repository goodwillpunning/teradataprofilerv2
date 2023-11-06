SELECT DATABASENAME,
    SUM((MAXPERM) /(1024 * 1024)) MAX_PERM_MB,
    SUM((CURRENTPERM) /(1024 * 1024)) CURRENT_PERM_MB,
    SUM((MAXSPOOL) /(1024 * 1024)) MAX_SPOOL_MB,
    SUM((CURRENTSPOOL) /(1024 * 1024)) CURRENT_SPOOL_MB
FROM DBC.DISKSPACEV
where DatabaseName NOT IN (
        'Crashdumps',
        'dbcmngr',
        'Default',
        'External_AP',
        'EXTUSER',
        'LockLogShredder',
        'PUBLIC',
        'Sys_Calendar',
        'SysAdmin',
        'SYSBAR',
        'SYSJDBC',
        'SYSLIB',
        'SystemFe',
        'SYSUDTLIB',
        'SYSUIF',
        'TD_SERVER_DB',
        'TDStats',
        'TD_SYSGPL',
        'TD_SYSXML',
        'TDMaps',
        'TDPUSER',
        'TDQCD',
        'tdwm',
        'SQLJ',
        'TD_SYSFNLIB',
        'SYSSPATIAL'
    )
GROUP BY DATABASENAME
