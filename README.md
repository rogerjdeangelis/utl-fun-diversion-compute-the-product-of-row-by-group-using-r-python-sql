# utl-fun-diversion-compute-the-product-of-row-by-group-using-r-python-sql
Fun diversion compute the product of rows by group using sql
    %let pgm=utl-fun-diversion-compute-the-product-of-row-by-group-using-r-python-sql;

    Fun diversion compute the product of rows by group using sql

    github                                                                                                     
    http://tinyurl.com/cw2tfsd9                                                                                
    https://github.com/rogerjdeangelis/utl-fun-diversion-compute-the-product-of-row-by-group-using-r-python-sql

    Compute the product of ages and weights by sex

       sOLUTIONS
          1 SAS sql  (exp(sum(log(age))) by sex is the product of ages
          2 r sql
          3 python sql

    /*               _     _
     _ __  _ __ ___ | |__ | | ___ _ __ ___
    | `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
    | |_) | | | (_) | |_) | |  __/ | | | | |
    | .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
    |_|
    */

    /**************************************************************************************************************************/
    /*                                       |                                            |                                   */
    /*              INPUT                    |       PROCESS                              |             OUTPUT                */
    /*                                       |                                            |                                   */
    /* SD1.HAVE total obs=5                  | SAME CODE IN SAS,R AND PYTHON              |                                   */
    /*                                       |                                            |                             PROC_ */
    /* Obs SEX AGE           WEIGHT          | proc sql;                                  |  Obs    SEX    PROD_AGE    WEIGHT */
    /*                                       |    create                                  |                                   */
    /*  1   F   14              90           |       table want as                        |   1      F       2520      776160 */
    /*  2   F   12              77           |    select                                  |   2      M        192       19200 */
    /*  3   F   15  14*12*15   112 90*77*112 |       sex                                  |                                   */
    /*              2520           776160    |      ,exp(sum(log(age)))    as prod_age    |                                   */
    /*                                       |      ,exp(sum(log(weight))) as proc_weight |                                   */
    /*  4   M   16             150           |    from                                    |                                   */
    /*  5   M   12  16*12      128 150*128   |       sd1.have                             |                                   */
    /*                                       |    group                                   |                                   */
    /**************************************************************************************************************************/

    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */

    options validvarname=upcase;
    libname sd1 "d:/sd1";
    data sd1.have;
      set sashelp.class(firstobs=12 OBS=16 );
      keep sex age weight;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  Obs    SEX    AGE    HEIGHT    WEIGHT                                                                                 */
    /*                                                                                                                        */
    /*   1      F      14     64.3        90                                                                                  */
    /*   2      F      12     56.3        77                                                                                  */
    /*   3      F      15     66.5       112                                                                                  */
    /*   4      M      16     72.0       150                                                                                  */
    /*   5      M      12     64.8       128                                                                                  */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*                               _
    / |  ___  __ _ ___    __ _  __ _| |
    | | / __|/ _` / __|  / _` |/ _` | |
    | | \__ \ (_| \__ \ | (_| | (_| | |
    |_| |___/\__,_|___/  \__,_|\__, |_|
                                  |_|
    */

    proc datasets lib=work nolist mt=data mt=view nodetails;delete want; run;quit;

    proc sql;
       create
          table want as
       select
          sex
         ,exp(sum(log(age)))    as prod_age
         ,exp(sum(log(weight))) as proc_weight
       from
          sd1.have
       group
          by sex;
    quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*                                                                                                                        */
    /*                             PROC_                                                                                      */
    /*  Obs    SEX    PROD_AGE    WEIGHT                                                                                      */
    /*                                                                                                                        */
    /*   1      F       2520      776160                                                                                      */
    /*   2      M        192       19200                                                                                      */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*___                     _
    |___ \   _ __   ___  __ _| |
      __) | | `__| / __|/ _` | |
     / __/  | |    \__ \ (_| | |
    |_____| |_|    |___/\__, |_|
                           |_|
    */

    %utlfkil(d:/xpt/want.xpt);

    %utl_rbegin;
    parmcards4;
    library(sqldf)
    library(haven)
    library(SASxport)
    have<-read_sas("d:/sd1/have.sas7bdat")
    want<-sqldf('
       select
          sex
         ,exp(sum(log(age)))    as prod_age
         ,exp(sum(log(weight))) as proc_weight
       from
          have
       group
          by sex
       ')
    want;
    for (i in seq_along(want)) {
              label(want[,i])<- colnames(want)[i];
           };
    write.xport(want,file="d:/xpt/want.xpt");
    ;;;;
    %utl_rend;

    proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;
    proc datasets lib=work nolist mt=data mt=view nodetails ;delete want; run;quit;

    /*--- handles long variable names by using the label to rename the variables  ----*/

    libname xpt xport "d:/xpt/want.xpt";
    proc contents data=xpt._all_;
    run;quit;

    data want_r_long_names;
      %utl_rens(xpt.want) ;
      set want;
    run;quit;
    libname xpt clear;

    proc print;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*                            PROC_                                                                                       */
    /* Obs    SEX    PROD_AGE    WEIGHT                                                                                       */
    /*                                                                                                                        */
    /*  1      F       2520      776160                                                                                       */
    /*  2      M        192       19200                                                                                       */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*____               _   _
    |___ /   _ __  _   _| |_| |__   ___  _ __
      |_ \  | `_ \| | | | __| `_ \ / _ \| `_ \
     ___) | | |_) | |_| | |_| | | | (_) | | | |
    |____/  | .__/ \__, |\__|_| |_|\___/|_| |_|
            |_|    |___/
    */

    %utlfkil(d:/xpt/res.xpt);

    proc datasets lib=work kill nodetails nolist;
    run;quit;

    %utlfkil(d:/xpt/res.xpt);

    %utl_pybegin;
    parmcards4;
    from os import path
    import pandas as pd
    import xport
    import xport.v56
    import pyreadstat
    import numpy as np
    import pandas as pd
    from pandasql import sqldf
    mysql = lambda q: sqldf(q, globals())
    from pandasql import PandaSQL
    pdsql = PandaSQL(persist=True)
    sqlite3conn = next(pdsql.conn.gen).connection.connection
    sqlite3conn.enable_load_extension(True)
    sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
    mysql = lambda q: sqldf(q, globals())
    have, meta = pyreadstat.read_sas7bdat("d:/sd1/have.sas7bdat")
    print(have);
    want = pdsql("""
       select
          sex
         ,exp(sum(log(age)))    as prod_age
         ,exp(sum(log(weight))) as proc_weight
       from
          have
       group
          by sex
    """)
    print(want);
    pyreadstat.write_xport(want,'d:\\xpt\\want.xpt',table_name='want',file_format_version=5
    ,column_labels=['country_id', 'country', 'last_update']);
    ;;;;
    %utl_pyend;

    proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;
    proc datasets lib=work nolist mt=data mt=view nodetails ;delete want; run;quit;

    /*--- handles long variable names by using the label to rename the variables  ----*/

    libname xpt xport "d:/xpt/want.xpt";
    proc contents data=xpt._all_;
    run;quit;

    data want_py_long_names;
      %utl_rens(xpt.want) ;
      set want;
    run;quit;
    libname xpt clear;

    proc print ;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*         COUNTRY_                LAST_                                                                                  */
    /*  Obs       ID       COUNTRY    UPDATE                                                                                  */
    /*                                                                                                                        */
    /*   1        F          2520     776160                                                                                  */
    /*   2        M           192      19200                                                                                  */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
