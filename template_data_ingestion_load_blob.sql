CREATE OR REPLACE PROCEDURE INGEST_STAGE_DIR_FILE(STAGE_DIR VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var starttime = new Date();

    var sql_statement = `use role accountadmin`;
    var sql_statement = `use schema ppurush_db_demo.sfc`;

    var sql_statement =
      `copy into mycsvtable
          (id, last_name, first_name, company, email, workphone, cellphone, streetaddress, city, postalcode,insert_timestamp)
      from
          (
          select
              $1,
              $2,
              $3,
              $4,
              $5,
              $6,
              $7,
              $8,
              $9,
              $10,
              CURRENT_TIMESTAMP()::TIMESTAMP_NTZ`;
    sql_statement += " FROM @" + STAGE_DIR + ")";
    var rs = snowflake.execute({sqlText: sql_statement});
    var endtime = new Date();
    var ro = [];
    // Loop through the results, processing one row at a time...
    while (rs.next())  {
       var rr = [];
       var message = rs.getColumnValue('status');
       var column1 = "";
       var column3 = "";
       var column4 = "";
       var column5 = "";
       var column6 = "";
       try {column1 = rs.getColumnValue('file');}catch(err){ message = rs.getColumnValue(1); column1 = "";};
       try {column3 = rs.getColumnValue('rows_parsed'); }catch(err){};
       try {column4 = rs.getColumnValue('rows_loaded'); }catch(err){};
       try {column5 = rs.getColumnValue('error_limit'); }catch(err){};
       try {column6 = rs.getColumnValue('errors_seen'); }catch(err){};
       // Do something with the retrieved values...
       rr.push(STAGE_DIR, message, column1, column3, column4, column5, column6, starttime, endtime, starttime-endtime);
       ro.push(rr);
       }
    return rr;
    //return sql_statement;
$$;

CALL INGEST_STAGE_DIR_FILE('STG_EXT_AZURE_BLOB_G1/dt=20210301');

CREATE OR REPLACE PROCEDURE INGEST_STAGE_DIR(STAGENAME string, STARTKEY DOUBLE, NUMKEYS DOUBLE, WHNAME string, WHSIZE string)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
 var return_rows = [];
 var number_days = NUMKEYS;
 var curr_key=STARTKEY;
 var stagedirbase = STAGENAME+'/dt='
 snowflake.execute({sqlText: "use warehouse " + WHNAME + ";"});
 snowflake.execute({sqlText: "alter warehouse " + WHNAME + " SET WAREHOUSE_SIZE = " + WHSIZE + ";"});
 for (i=0; i<number_days; i++)
 {
    var starttime = new Date();
    var stagedir = stagedirbase + curr_key.toString() + "/";
    var sp_caller = "call INGEST_STAGE_DIR_FILE('" + stagedir + "')";
    var result = snowflake.execute({sqlText: sp_caller});
    var endtime = new Date();
    result.next();
    var rtn = result.getColumnValue(1);
    var resultset = [];
    resultset.push(WHNAME, WHSIZE, stagedir,endtime-starttime, rtn);
    return_rows.push(resultset);
    curr_key=curr_key + 1;
 }
 return return_rows;
$$;

CALL INGEST_STAGE_DIR('STG_EXT_AZURE_BLOB_G1',20210301,5,'PPURUSH_WH','XSMALL');


/* Syntax for Parquet
var sql_statement =
      `copy into <TABLE_NAME>
          (columns list here)
      from
          (
          select
              $1:_col0::string as origimsi,
              $1:_col1::string as origimei,
              $1:_col2::string as toc,
              $1:_col3::string as starttime,
              $1:_col4::string as endtime,
              $1:_col5::string as termimsi,
              $1:_col6::string as termimei,
              $1:_col7::string as imsi,
              $1:_col8::string as msisdn,
              $1:_col9::string as currentsac,
              $1:_col10::string as anumber,
              $1:_col11::string as bnumber,
              $1:_col12::string as ranapcause,
              $1:_col13::string as cccause,
              $1:_col14::string as mmcause,
              $1:_col15::string as setuptime,
              $1:_col16::string as alerttime,
              $1:_col17::string as connecttime,
              $1:_col18::string as conacktime,
              $1:_col19::integer as callsetupdurms,
              $1:_col20::integer as callconvdurms,
              $1:_col18::string as pagingcause,
              $1:_col22::integer as nbofpgreqs,
              $1:_col23::string as rat,
              $1:_col24::string as iuscalltype,
              to_date(NULLIF(regexp_replace(METADATA$FILENAME,'.*\\\\=(.*)\\\\/.*','\\\\1'),'__HIVE_DEFAULT_PARTITION__'),'YYYY-MM-DD') as dt`;
*/
