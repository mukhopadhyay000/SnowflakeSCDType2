-- Stored Procedure on change apply delete for CDC Type2 to hard delete from STG w/ to the recodrs deleted in the source. This SP will be used with the full file.
-- Proc Name : SP_CHANGE_APPLY_DELETE_STG
-- Input Parameters
--      A. Database Name
--      B. Source Schema
--		C. Source Table Name
--		D. Stage Schema
--		E. Stage Table Name
--      F. Target Schema Name
--		G. Target Table Name
-- Author : Anup Mukhopadhyay (IBM Consultant) 

CREATE OR REPLACE PROCEDURE SP_CHANGE_APPLY_DELETE_STG(DB VARCHAR, SRC_SCHMA VARCHAR, SRC_TABLE VARCHAR, STG_SCHMA VARCHAR, STG_TABLE VARCHAR, TGT_SCHMA VARCHAR, TGT_TABLE VARCHAR)
RETURNS  TEXT NOT NULL
LANGUAGE JAVASCRIPT
COMMENT = 'SP will generates dynamic merge statement to apply delete in stage table'
EXECUTE AS OWNER
AS
$$

    var ky_col = [];
    var ky1 = [];

	try {

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
						ky1.push('S.'+ split_key[i]);

					}
				var ky = ky1.join(', ');

            var del_sql = "DELETE FROM "
			del_sql += STG_SCHMA + "." + STG_TABLE + " S "
			del_sql += "WHERE "
			del_sql += "(" + ky1 + ") in (SELECT "
			del_sql += ky_col + " FROM ("
			del_sql += "SELECT " + ky_col + " FROM " + STG_SCHMA + "." + STG_TABLE
			del_sql += " minus "
			del_sql += "SELECT " + ky_col + " FROM " + TGT_SCHMA + "." + TGT_TABLE
			del_sql += " WHERE CRNT_FLG = 'Y'));" 

				
			return del_sql;
	}
	catch (err) {
		return "Error :" + err;
	}
$$;

-- call SP_CHANGE_APPLY_DELETE_STG('PXCF_GTS_DEV_DB, 'GTS_STG', SRC_TABLE VARCHAR, 'GTS_STG', 'ING_PX_POS_RAW, 'GTS_TRFN', 'TRFN_PX_POS')