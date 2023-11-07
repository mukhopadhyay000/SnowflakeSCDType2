-- Stored Procedure on change capture for CDC Type2
-- Proc Name : SP_CHANGE_CAPTURE
-- Input Parameters
--      A. Source Table Name
--		B. Stage Table Name
--		C. Database Name
--		D. Schema Name
-- Author : Anup Mukhopadhyay (IBM Consultant) 

/*##############################################################
Change Tracking
	1.	08/10/2023 added new NVL condition to the column matching 
		logic to solve the comparing issue some value with NULL value.


--############################################################## */


CREATE OR REPLACE PROCEDURE GTS_STG.SP_CHANGE_CAPTURE(SRC_TABLE VARCHAR, TGT_TABLE VARCHAR, DB VARCHAR, SCHMA VARCHAR)
RETURNS  TEXT NOT NULL
LANGUAGE JAVASCRIPT
COMMENT = 'SP will generates dynamic merge statement to identify delta'
EXECUTE AS OWNER
AS
$$
    var sel_col = [];
    var update_col = [];
    var insert_col = [];
    var insert_val = [];
    var match_col = [];
    var ky_col = [];
    var filter_cond = [];
    var sel_sql = "select * from " + DB + "." + SCHMA + ".C_ING_PX_CDC_CONFIG WHERE TBL_NM = '"+ SRC_TABLE +"' and TYP = 'COLUMN_NAME' ORDER BY ORDINAL_POSITION;";
    var sel_sql2 = "select COL_NM FROM "+ DB + "." + SCHMA + ".C_ING_PX_CDC_CONFIG WHERE TBL_NM = '"+ SRC_TABLE +"' and TYP = 'MRG_STG_KY' ORDER BY ORDINAL_POSITION;";
    var sel_stmet = snowflake.createStatement({sqlText: sel_sql});
    var sel_stmet2 = snowflake.createStatement({sqlText: sel_sql2});

    try{
        var sel_res = sel_stmet.execute();
        var sel_res2 = sel_stmet2.execute();
        while (sel_res2.next()){
           ky_col.push(sel_res2.getColumnValue(1));
        }
         var x = ky_col.toString(); 
         var split_key = x.split(',');
      
        var i;
			for (i = 0; i < split_key.length; i++)
				{
					filter_cond.push('t1.'+ split_key[i] + '= t2.'+ split_key[i])
				}
     
        while (sel_res.next()){
           sel_col.push(sel_res.getColumnValue(4));
           var1 = sel_res.getColumnValue(6);
            if(var1 == 'Y') {
               update_col.push('t1.'+ sel_res.getColumnValue(4) + ' = t2.' + sel_res.getColumnValue(4));
			}
            var8 = sel_res.getColumnValue(4);
			var9 = sel_res.getColumnValue(5);
            if(var1 == 'Y' && var8 != 'SNPST_DT') {
				if (var9 == 'TEXT') {
					match_col.push('(NVL(t1.'+ sel_res.getColumnValue(4) + ', \'X\') != NVL(t2.' + sel_res.getColumnValue(4) + ', \'X\'))');
				}
				else if (var9 == 'DATE') {
					match_col.push('(NVL(t1.'+ sel_res.getColumnValue(4) + ', \'1900-01-01\') != NVL(t2.' + sel_res.getColumnValue(4) + ', \'1900-01-01\'))');
				}
				else if (var9 == 'NUMBER') {
					match_col.push('(NVL(t1.'+ sel_res.getColumnValue(4) + ', \'0.00\') != NVL(t2.' + sel_res.getColumnValue(4) + ', \'0.00\'))');
				}
				else if (var9 == 'TIMESTAMP_NTZ') {
					match_col.push('(NVL(t1.'+ sel_res.getColumnValue(4) + ', \'1900-01-01 00:00:00\') != NVL(t2.' + sel_res.getColumnValue(4) + ', \'1900-01-01 00:00:00\'))');
				}				
				else {
						
				}					
            }
			insert_col.push(sel_res.getColumnValue(4));
            insert_val.push(sel_res.getColumnValue(4));
        }

			fin_sel_col = sel_col.join(',');
			fin_update_col = update_col.join(',');
			fin_insert_col = insert_col.join(',');
			fin_insert_val = insert_val.join(',');
			fin_match_val = match_col.join(' OR ');
			fin_filter_cond = filter_cond.join(" AND ");

			var mrg_sql = "MERGE INTO "
			mrg_sql += DB + "." + SCHMA + "." + TGT_TABLE + " t1 "
			mrg_sql += "USING "
			mrg_sql += "(SELECT "
			mrg_sql +=  fin_sel_col + " FROM " + DB + "." + SCHMA + "." + SRC_TABLE + ") t2 "
			mrg_sql += "ON "
			mrg_sql += fin_filter_cond
			mrg_sql += " WHEN MATCHED and "
			mrg_sql += "(" + fin_match_val + ") "
			mrg_sql += "THEN UPDATE SET "
			mrg_sql += fin_update_col
			mrg_sql +=" WHEN NOT MATCHED THEN INSERT "
			mrg_sql += "(" + fin_insert_col + ") "
			mrg_sql += "VALUES "
			mrg_sql += "(" + fin_insert_val + ")"

		return (mrg_sql);
    }
    catch(err){
		return 'Error :' + err;
		}
$$;