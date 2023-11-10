-- Stored Procedure on change apply for CDC Type2
-- Proc Name : SP_CHANGE_APPLY
-- Input Parameters
--      A. Stream Name
--		B. Target Table Name
--		C. Database Name
--		D. Schema name
--		E. Source Table Name
-- Author : Anup Mukhopadhyay (IBM Consultant) 

CREATE OR REPLACE PROCEDURE GTS_STG.SP_CHANGE_APPLY(STREAM_NM VARCHAR, TGT_SCHMA VARCHAR, TGT_TABLE VARCHAR, DB VARCHAR, SRC_SCHMA VARCHAR, SRC_TABLE VARCHAR)
RETURNS  TEXT NOT NULL
LANGUAGE JAVASCRIPT
COMMENT = 'SP will generates dynamic merge statement to apply upsert'
EXECUTE AS OWNER
AS
$$
    var sel_col = [];
    var update_col = [];
    var update_val = [];
    var insert_col = [];
    var insert_val = [];
    var ky_col = [];
    var filter_cond = [];
    var update_cond = [];
    var load_dt 
	
	//get snapshot date
    
	var snp_dt_qry = "select distinct TO_CHAR(max(SNPST_DT)) from " + DB + "." + SRC_SCHMA + "." + SRC_TABLE + ";";
    var snp_dt_cmd = snowflake.createStatement({sqlText: snp_dt_qry});
    var snp_dt_obj = snp_dt_cmd.execute();
    
	while (snp_dt_obj.next()){
		var snp_dt = snp_dt_obj.getColumnValue(1);
	}
	
	// get previous snapshot date
	
	var prev_snp_dt_qry = `select to_char(DATEADD(DAY, -1, '${snp_dt}'), 'YYYY-MM-DD');`;
    var prev_snp_dt_cmd = snowflake.createStatement({sqlText: prev_snp_dt_qry});
    var prev_snp_dt_obj = prev_snp_dt_cmd.execute();
    
	while (prev_snp_dt_obj.next()){
		var prev_snp_dt = prev_snp_dt_obj.getColumnValue(1);
	}
	
	//get load date
    
    var load_dt_qry = "select to_char(current_timestamp);";
    var load_dt_cmd = snowflake.createStatement({sqlText: load_dt_qry});
    var load_dt_obj = load_dt_cmd.execute();
    
	while (load_dt_obj.next()){
		var load_dt = load_dt_obj.getColumnValue(1);
	}
    
	var sel_sql1 = "select * from " + DB + "." + SRC_SCHMA + ".C_ING_PX_CDC_CONFIG WHERE TBL_NM = '"+ TGT_TABLE +"' and TYP = 'COLUMN_NAME' ORDER BY ORDINAL_POSITION;";
    var sel_sql2 = "select COL_NM FROM "+ DB + "." + SRC_SCHMA + ".C_ING_PX_CDC_CONFIG WHERE TBL_NM = '"+ TGT_TABLE +"' and TYP = 'MRG_KY' ORDER BY ORDINAL_POSITION;";
    var sel_sql3 = "select * from " + DB + "." + SRC_SCHMA + "." + STREAM_NM + ";";
    var sel_upd_sql = "select COL_NM, case when COL_NM = 'CRNT_FLG' THEN 'N' when COL_NM = 'EFFCTV_TO_TMS' THEN '" + prev_snp_dt +"' else NULL end as COL_VAL FROM " + DB +"." + SRC_SCHMA + "." + "C_ING_PX_CDC_CONFIG WHERE TBL_NM = '"+ TGT_TABLE +"' AND UPDATE_FLG = 'Y' ORDER BY ORDINAL_POSITION;";
   
    
    var sel_stmt1 = snowflake.createStatement({sqlText: sel_sql1});
    var sel_stmt2 = snowflake.createStatement({sqlText: sel_sql2});
    var sel_stmt3 = snowflake.createStatement({sqlText: sel_sql3});
    var sel_upd_stmt = snowflake.createStatement({sqlText: sel_upd_sql});
    
    
    var queryText1 = sel_stmt1.getSqlText();
    var queryText2 = sel_stmt2.getSqlText();
    var queryText3 = sel_upd_stmt.getSqlText();
    
    try{
        var sel_res = sel_stmt1.execute();
        var sel_res2 = sel_stmt2.execute();
        var sel_res3 = sel_stmt3.execute();
        var sel_res4 = sel_upd_stmt.execute();
        
        while (sel_res2.next()){
            ky_col.push(sel_res2.getColumnValue(1));
        }
            var x = ky_col.toString();  
            var split_key = x.split(',');
        
            var i;
				for (i = 0; i < split_key.length; i++){
					filter_cond.push('t1.'+ split_key[i] + ' = t2.'+ split_key[i]);
				}
           
        while (sel_res.next()){
           sel_col.push(sel_res.getColumnValue(4));
           insert_col.push(sel_res.getColumnValue(4));
           var k = sel_res.getColumnValue(4);
            if(k != 'EFFCTV_FRM_TMS' && k != 'EFFCTV_TO_TMS' && k != 'CRNT_FLG' && k != 'LOAD_DT'){
                insert_val.push(sel_res.getColumnValue(4));
            }
        }
        
        while (sel_res4.next()){
            update_col.push(sel_res4.getColumnValue(1));
            update_val.push(sel_res4.getColumnValue(2));
       }     
            var y = update_col.toString();
            var split_col = y.split(',');
            
            var z = update_val.toString();
            var split_val = z.split(',');
 
            var j;
            for (j = 0; j < split_col.length; j++){
                update_cond.push('t1.'+ split_col[j] + " = '" + split_val[j] + "'");
             }     
        
			fin_sel_col = sel_col.join(',');
			fin_insert_col = insert_col.join(',');
			fin_insert_val = insert_val.join(',');
			fin_filter_cond = filter_cond.join(" AND ");
			fin_update_cond = update_cond.join(', ');
    
			var mrg_sql = "MERGE INTO "
			mrg_sql += DB + "." + TGT_SCHMA + "." + TGT_TABLE + " t1 " 
			mrg_sql += "USING "
			mrg_sql += "(SELECT "
			mrg_sql +=  "*" + " FROM " + DB + "." + SRC_SCHMA + "." + STREAM_NM + ") t2 " 
			mrg_sql += "ON " 
			mrg_sql += fin_filter_cond
			mrg_sql += " WHEN MATCHED AND (t2.METADATA$ACTION = 'DELETE')"
			mrg_sql += " THEN UPDATE SET "
			mrg_sql += fin_update_cond
			mrg_sql +=" WHEN NOT MATCHED AND (t2.METADATA$ACTION = 'INSERT') THEN INSERT " 
			mrg_sql += "(" + fin_insert_col + ")"
			mrg_sql += " VALUES " 
			mrg_sql += "(" + fin_insert_val
			mrg_sql += ",TO_TIMESTAMP_NTZ('" + snp_dt + "'),TO_DATE('9999-12-31'),'Y',TO_TIMESTAMP_NTZ('" + load_dt + "'));"
    
		return (mrg_sql);
    }   
    catch(err){
        return 'Error :' + err;
    }
$$;