SELECT 'Detail' (CHAR(7)) as LogType,
    A.LogDate,
    CAST ((A.LogDate (FORMAT 'eee')) AS CHAR(3)) AS DOW,
    extract(
        hour
        from FirstStepTime
    ) as HR,
    cast(
        cast((logdate (format 'yyyy-mm-dd')) as char(10)) || ' ' || CAST(
            extract(
                hour
                from FirstStepTime
            ) AS DECIMAL(2) FORMAT '99'
        ) || ':00:00' as timestamp(0)
    ) LogHour_TS,
    U.Organization,
    U.Department,
    A.WdName,
    A.AcctString,
    A.Username,
    from_bytes(A.userId, 'base16') as userId,
    CASE
      WHEN upper(A.appid) like 'JDBC%' THEN 'JDBC'
      ELSE AppId
    END as AppId,
    CASE
        WHEN instr(upper(StatementGroup), 'DDL CREATE') > 0 and instr(upper(QueryText), 'SELECT') > 0 THEN 'ETL'
        WHEN instr(upper(StatementGroup), 'DML') > 0 and instr(upper(StatementGroup), 'INSERT') > 0 THEN 'ETL'
        WHEN instr(upper(StatementGroup), 'DML') > 0 and instr(upper(StatementGroup), 'UPDATE') > 0 THEN 'ETL'
        WHEN instr(upper(StatementGroup), 'DML') > 0 and instr(upper(StatementGroup), 'DELETE') > 0 THEN 'ETL'
        WHEN instr(upper(StatementGroup), 'DML') > 0 and instr(upper(StatementGroup), 'DEL=') > 0 and instr(upper(StatementGroup), 'DEL=0') = 0 THEN 'ETL'
        WHEN instr(upper(StatementGroup), 'DML') > 0 and instr(upper(StatementGroup), 'INS=') > 0 and instr(upper(StatementGroup), 'INS=0') = 0 THEN 'ETL'
        WHEN instr(upper(StatementGroup), 'DML') > 0 and instr(upper(StatementGroup), 'INSSEL=') > 0 and instr(upper(StatementGroup), 'INSSEL=0') = 0 THEN 'ETL'
        WHEN instr(upper(StatementGroup), 'DML') > 0 and instr(upper(StatementGroup), 'UPD=') > 0 and instr(upper(StatementGroup), 'UPD=0') = 0 THEN 'ETL'
        WHEN upper(appid) in ('FASTEXP', 'MULTLOAD', 'FASTLOAD') THEN 'ETL'
        WHEN instr(upper(StatementGroup), 'SELECT') > 0 or instr(upper(QueryText), 'SELECT') = 1 THEN 'BI/QUERY'
        --WHEN instr(upper(StatementGroup), 'DDL') > 0 and instr(upper(StatementGroup), 'GRANT') > 0 THEN 'DCL'
        WHEN instr(upper(StatementGroup), 'DDL') > 0 THEN 'DDL'
        WHEN instr(upper(StatementType), 'PROCEDURE') > 0 and (instr(upper(StatementType), 'CREATE') > 0 or instr(upper(StatementType), 'REPLACE') >0 ) THEN 'DML'
        --WHEN instr(upper(StatementType), 'COLLECT STATISTICS') > 0 ''
        WHEN instr(upper(StatementType), 'CALL') > 0 THEN 'SP'
        ELSE 'OTHER'
    END as QueryType,
    count (*) as QryCNT,
    sum(a.AMPCPUTime) as SumCPU,
    avg(a.AMPCPUTime) as AvgCPU,
    max(a.AMPCPUTime) as MaxCPU,
    sum(a.TotalIOCOUNT) as SumIO,
    avg(a.TotalIOCOUNT) as AvgIO,
    max(a.TotalIOCOUNT) as MaxIO,
    Max(A.DelayTime) as MaxTDWMDelayTime,
    Sum(A.DelayTime) as SumTDWMDelayTime,
    avg(
        Zeroifnull(
            CAST(
                extract(
                    hour
                    From (
                            (a.Firstresptime - a.StartTime) DAY(4) TO SECOND(6)
                        )
                ) * 3600 + extract(
                    minute
                    From (
                            (a.Firstresptime - a.StartTime) DAY(4) TO SECOND(6)
                        )
                ) * 60 + extract(
                    second
                    From (
                            (a.Firstresptime - a.StartTime) DAY(4) TO SECOND(6)
                        )
                ) AS Decimal(10, 2)
            )
        )
    ) as AvgRespSecs,
    max(
        Zeroifnull(
            CAST(
                extract(
                    hour
                    From (
                            (a.Firstresptime - a.StartTime) DAY(4) TO SECOND(6)
                        )
                ) * 3600 + extract(
                    minute
                    From (
                            (a.Firstresptime - a.StartTime) DAY(4) TO SECOND(6)
                        )
                ) * 60 + extract(
                    second
                    From (
                            (a.Firstresptime - a.StartTime) DAY(4) TO SECOND(6)
                        )
                ) AS Decimal(10, 2)
            )
        )
    ) as MaxRespSecs
FROM pdcrinfo.dbqlogtbl_hst a
    LEFT JOIN pdcrinfo.UserInfo U ON a.UserName = U.UserName
WHERE (
        a.AMPCPUTime > 0
        OR a.TotalIOCount > 0
    )
    AND a.NumSteps > 0
    AND a.LogDate >= date - ${pdcr_history_days}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
UNION ALL
SELECT 'Summary' (CHAR(7)) as LogType,
    b.LogDate,
    CAST ((b.LogDate (FORMAT 'eee')) AS CHAR(3)) AS DOW,
    extract(
        hour
        from b.collectTimestamp
    ) as HR,
    cast(
        cast((logdate (format 'yyyy-mm-dd')) as char(10)) || ' ' || CAST(
            extract(
                hour
                from b.collectTimestamp
            ) AS DECIMAL(2) FORMAT '99'
        ) || ':00:00' as timestamp(0)
    ) LogHour_TS,
    U.Organization,
    U.Department,
    cast(null as varchar(128)) as WdName,
    b.AcctString,
    b.username,
    from_bytes(b.userId, 'base16') as userId,
    'Tactical' as AppId,
    'Tactical' as QueryType,
    sum(b.querycount) as QryCNT,
    sum(b.ampcputime + b.parsercputime) as SumCPU,
    sum(b.ampcputime + b.parsercputime) / sum(b.querycount) as AvgCPU,
    null as MaxCPU,
    sum(b.TotalIOCOUNT) as SumIO,
    sum(b.TotalIOCOUNT) / sum(b.querycount) as AvgIO,
    null as MaxIO,
    -1 as MaxTDWMDelayTime,
    null as SumTDWMDelayTime,
    sum(b.queryseconds) / sum(b.querycount) as AvgRespSecs,
    cast(null as decimal(10, 2)) as MaxRespSecs
FROM pdcrinfo.dbqlsummarytbl_hst b
    LEFT JOIN pdcrinfo.UserInfo U ON b.UserName = U.UserName
WHERE b.LogDate >= date - 180
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
