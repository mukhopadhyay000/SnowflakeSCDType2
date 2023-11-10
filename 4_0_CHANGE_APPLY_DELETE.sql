-- Stored Procedure on change apply delete for CDC Type2 to soft delete the recodrs deleted in the source. This SP will be used with the full file.
-- Proc Name : SP_CHANGE_APPLY_DELETE
-- Input Parameters
--      A. Database Name
--		B. Source Schema Name
--		C. Target Schema Name
--		D. Source Table Name
--		E. Target Table Name
-- Author : Anup Mukhopadhyay (IBM Consultant) 

CREATE OR REPLACE PROCEDURE SP_CHANGE_APPLY_DELETE(DB VARCHAR, SRC_SCHMA VARCHAR, TGT_SCHMA VARCHAR, SRC_TABLE VARCHAR, TGT_TABLE VARCHAR)
RETURNS  TEXT NOT NULL
LANGUAGE JAVASCRIPT
COMMENT = 'SP will generates dynamic merge statement to apply upsert'
EXECUTE AS OWNER
AS
$$

    var ky_col = [];
    var join_key1 = [];
	var join_key2 = [];
	var in_join_key = [];
	var out_join_key = [];
	var select_col = [];
	var fin_select_col = [];
	var update_cond = [];
    var fin_update_cond = [];

	try {

			//var sel_sql1 = "select COL_NM FROM "+ DB + "." + SRC_SCHMA + ".C_ING_PX_CDC_CONFIG WHERE TBL_NM = '"+ SRC_SCHMA + "'." + SRC_TABLE +"' and TYP = 'MRG_STG_KY' ORDER BY ORDINAL_POSITION;";
			var sel_sql1 = `select COL_NM FROM ${DB}.${SRC_SCHMA}.C_ING_PX_CDC_CONFIG WHERE TBL_NM = '${SRC_TABLE}' and TYP = 'MRG_STG_KY' ORDER BY ORDINAL_POSITION;`			

			var sel_stmt1 = snowflake.createStatement({sqlText: sel_sql1});
			
			var sel_res1 = sel_stmt1.execute();
			
			while (sel_res1.next()) {
				ky_col.push(sel_res1.getColumnValue(1));
			}
				var x = ky_col.toString();  
				var split_key = x.split(',');
			
				var i;
					for (i = 0; i < split_key.length; i++){
						join_key1.push('A.'+ split_key[i] + ' = B.'+ split_key[i]);
						join_key2.push('X.'+ split_key[i] + ' = Y.'+ split_key[i]);
						select_col.push('B.' + split_key[i] + ' as D_' + split_key[i]);
						update_cond.push('C.D_' + split_key[i] + " IS NULL");
					}
				in_join_key = join_key1.join(' AND ');
				out_join_key = join_key2.join(' AND ');
				fin_select_col= select_col.join(' , ');
				fin_update_cond = update_cond.join(' AND ');
				
			var mrg_sql = "MERGE INTO "
			mrg_sql += TGT_SCHMA + "." + TGT_TABLE + " X " 
			mrg_sql += "USING (SELECT "
			mrg_sql += "C.* FROM ("
			mrg_sql += "SELECT A.*, "
			mrg_sql += fin_select_col
			mrg_sql += " FROM (SELECT * FROM " + TGT_SCHMA + "." + TGT_TABLE + " WHERE CRNT_FLG = 'Y') A "
			mrg_sql += "LEFT OUTER JOIN " + SRC_SCHMA + "." + SRC_TABLE + " B ON "
			mrg_sql += in_join_key
			mrg_sql += " WHERE CRNT_FLG = 'Y') C "
			mrg_sql += "WHERE " + fin_update_cond + ") Y "
			mrg_sql += "ON " + out_join_key
			mrg_sql += " AND X.EFFCTV_TO_TMS = '9999-12-31' AND X.CRNT_FLG = 'Y' "
			mrg_sql += " WHEN MATCHED THEN UPDATE SET "
			mrg_sql += "EFFCTV_TO_TMS = TO_TIMESTAMP_NTZ(dateadd(day,+0,TO_DATE(X.SNPST_DT)))"
			mrg_sql += ",X.CRNT_FLG = 'N';"
				
			return mrg_sql;
	}
	catch (err) {
		return "Error :" + err;
	}
$$;