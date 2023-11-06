select databasename,
    sum(currentperm) / 1024 / 1024 / 1024 / 1024 as curretperm_tb,
    sum(uncompressed_currentperm) / 1024 / 1024 / 1024 / 1024 as uncompressed_on_td_tb,
    sum(cds) / 1024 / 1024 / 1024 / 1024 as raw_tb
from (
        sel ts.databasename,
        ts.tablename,
        ts.currentperm,
        ts.currentperm_minus_fb,
        blc.blccompratio,
        ts.currentperm / coalesce(nullifzero((1 - (blc.blccompratio / 100.00))), 1) as uncompressed_currentperm,
        ts.currentperm_minus_fb / coalesce(nullifzero((1 - (blc.blccompratio / 100.00))), 1) as cds,
        case
            when blc.databasename is null then 'N'
            else 'Y'
        end as blc_flag
        from (
                sel ts1.databasename,
                ts1.tablename,
                sum(currentperm) (decimal(20, 0)) as currentperm,
                sum(
                    case
                        when t1.ProtectionType = 'F' then currentperm / 2
                        else currentperm
                    end
                ) (decimal(20, 0)) as currentperm_minus_fb
                from dbc.tablesizev ts1
                    join dbc.tablesv t1 on ts1.databasename = t1.databasename
                    and ts1.tablename = t1.tablename
                group by 1,
                    2
            ) ts
            left join (
                sel databasename,
                tablename,
                statssource,
                blccompratio
                from dbc.tablestatsv
                where columnname is null
                    and blccompratio > 0
            ) blc on ts.databasename = blc.databasename
            and ts.tablename = blc.tablename
    ) dtl
group by 1
