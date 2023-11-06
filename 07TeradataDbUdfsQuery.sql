SELECT C.DatabaseName,
    FunctionName,
    NumParameters,
    ParameterDataTypes,
    CASE
        SrcFileLanguage
        WHEN 'S' THEN 'SQL'
        WHEN 'C' THEN 'C'
        WHEN 'P' THEN 'C++'
        WHEN 'J' THEN 'JAVA'
        WHEN 'A' THEN 'SAS'
    END AS FunctionLanguage,
    CASE
        FunctionType
        WHEN 'A' THEN 'Aggregate'
        WHEN 'B' THEN 'Aggregate and statistical'
        WHEN 'C' THEN 'Contract function'
        WHEN 'F' THEN 'Scalar'
        WHEN 'H' THEN 'User-defined method'
        WHEN 'I' THEN 'Internal type method'
        WHEN 'L' THEN 'Table operator'
        WHEN 'R' THEN 'Table'
        WHEN 'S' THEN 'Statistical'
    END as FunctionType,
    ColumnType AS ReturnType
FROM DBC.FunctionsV T
    LEFT JOIN DBC.ColumnsV C ON C.DatabaseName = T.DatabaseName
    AND C.TableName = T.SpecificName
    AND ColumnName = 'RETURN0'
WHERE T.DatabaseName NOT IN (
        'All',
        'Crashdumps',
        'DBC',
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
  
