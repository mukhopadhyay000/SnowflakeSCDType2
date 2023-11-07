-- Stored Procedure to insert VALUES in the CDC Config tables.
-- Proc Name : CRT_CDC_CONFIG
-- Input Parameters
    --DB - Database Name
	--SCHMA - Schema Name
	-- TABLE_NM - Table Name
	-- KEYS - Key Names
	-- KEY_TYPE - Type of Key (MRG_STG_KEY/MRG_KEY)
	-- TABLE_TYPE - Type of Table (RAW-R/TRFN-T)
-- Author : Anup Mukhopadhyay (IBM Consultant)

CREATE OR REPLACE PROCEDURE <deployment database name>.GTS_STG.CRT_CDC_CONFIG(DB VARCHAR, SCHMA VARCHAR, TABLE_NM VARCHAR, KEYS VARCHAR, KEY_TYPE VARCHAR, TABLE_TYPE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
	var assign_cond = [];
	var insert_cols = [];
	var insert_keys = [];
	var cols = [];
	var table_type = TABLE_TYPE;
	
	try {
		var v_step=10;
		
		var sql_qry = `select * from INFORMATION_SCHEMA.COLUMNS where table_name = 'C_ING_PX_CDC_CONFIG' order by ORDINAL_POSITION`;
		var sql_stmt = snowflake.createStatement({sqlText: sql_qry}).execute();
        while (sql_stmt.next()){
           insert_cols.push(sql_stmt.getColumnValue(4));
		   cols = insert_cols.slice(1);
		fin_insert_cols = cols.join(", ");
		}
	}
	catch (err) {
		return "Error in step - " +v_step+ ": (" + err.code + ")" + err.message;
	}

	try {
		var v_step=20;
		
		var msk = KEYS.split(';');
        var i;
			for (i = 0; i < msk.length; i++){
				assign_cond.push("WHEN COLUMN_NAME = '" + msk[i] + "' THEN 'N'");
	}
		fin_assign_cond = assign_cond.join(" ");
		//return fin_assign_cond;
	}
	catch (err) {
		return "Error in step - " +v_step+ ": (" + err.code + ")" + err.message;
	}
	
	try {
		var v_step=30;
		
		var sql_ins = "INSERT INTO "
		sql_ins += DB + "." + SCHMA + "." + "C_ING_PX_CDC_CONFIG ("
		sql_ins += fin_insert_cols + ") "
		sql_ins += "SELECT TABLE_NAME, 'COLUMN_NAME' AS TYP, COLUMN_NAME, DATA_TYPE, "
		sql_ins += "'Y' AS INSERT_FLG, "
		sql_ins += "CASE "
		sql_ins += fin_assign_cond
		sql_ins += " ELSE 'Y' END AS UPDATE_FLG, "
		sql_ins += "ORDINAL_POSITION "
		sql_ins += "FROM INFORMATION_SCHEMA.COLUMNS "
		sql_ins += "WHERE TABLE_NAME = '" + TABLE_NM + "' "
		sql_ins += "ORDER BY ORDINAL_POSITION;"
		
	//return sql_ins;
	}
	catch (err) {
		return "Error in step - " +v_step+ ": (" + err.code + ")" + err.message;
	}
	
	try {
		var v_step=40; // execute column insert into CDC config table

		var ins_qry1 = snowflake.createStatement({sqlText: sql_ins});
		var ins_stmt1 = ins_qry1.execute();
		ins_stmt1.next();
			var message1 = "Inserted column records: " + ins_stmt1.getColumnValue(1);
	
	//return message1;
	}
	catch (err) {
		return "Error in step - " +v_step+ ": (" + err.code + ")" + err.message;
	}

	try {
		var v_step=50; //get max ordinal position of the inserted records for the table
		
		var max_qry1 = `select max(ORDINAL_POSITION) from ${DB}.${SCHMA}.C_ING_PX_CDC_CONFIG where TBL_NM = '${TABLE_NM}';`;
		var max_stmt1 = snowflake.createStatement({sqlText: max_qry1}).execute();
		max_stmt1.next();
			var max_val = max_stmt1.getColumnValue(1);
	
	//return max_val;
	}
	catch (err) {
		return "Error in step - " +v_step+ ": (" + err.code + ")" + err.message;
	}
	
	try {
		var v_step=60; //create insert statements for the keys
		
        var i;
			for (i = 1; i <= msk.length; i++){
                val = (max_val +i);
				ins_qry2 = "INSERT INTO " + DB + "." + SCHMA + "." + "C_ING_PX_CDC_CONFIG(" + fin_insert_cols + ")" + " VALUES ('" + TABLE_NM + "','" + KEY_TYPE + "','" + msk[i-1] + "','NA','N','N'," + val + ");";
				ins_stmt2 = snowflake.createStatement({sqlText: ins_qry2}).execute();
				ins_stmt2.next();
					var message2 = "Inserted key records: " + i;
				//insert_keys.push("INSERT INTO " + DB + "." + SCHMA + "." + "C_ING_PX_CDC_CONFIG(" + fin_insert_cols + ")" + " VALUES ('" + TABLE_NM + "','MRG_STG_KY','" + msk[i-1] + "','N','Y'," + val + ");");
				//fin_insert_keys = insert_keys.join(" ");				
			}
		//return message2;
	}
	catch (err) {
		return "Error in step - " +v_step+ ": (" + err.code + ")" + err.message;
	}
	
	if (table_type == 'T') {
		try {
			var v_step=70; // update column values for updates in transformation table
			
			upd_qry1 = `UPDATE C_ING_PX_CDC_CONFIG SET UPDATE_FLG = 'N' WHERE TBL_NM = '${TABLE_NM}';`;
			upd_stmt1 = snowflake.createStatement({sqlText: upd_qry1}).execute();
			upd_stmt1.next()
				upd_result1 = upd_stmt1.getColumnValue(1);
				var message3 = "Updated update column with update query1";
		
			upd_qry2 = `UPDATE C_ING_PX_CDC_CONFIG SET UPDATE_FLG = 'Y' WHERE TBL_NM = '${TABLE_NM}' AND COL_NM IN ('EFFCTV_TO_TMS','CRNT_FLG');`;
			upd_stmt2 = snowflake.createStatement({sqlText: upd_qry2}).execute();
			upd_stmt2.next()
				upd_result2 = upd_stmt2.getColumnValue(1);
				var message4 = "Updated update column with update query2";
		
		return message1 + "/" + message2 + "/" + message3 + "/" + message4;
		}
		catch (err) {
			return "Error in step - " +v_step+ ": (" + err.code + ")" + err.message;
		}
	}
	else if (table_type == 'R') {
		try {
			var message3 = "No update required"
		
		return message1 + "/" + message2 + "/" + message3;
		}
		catch (err) {
			return "Error in step - " +v_step+ ": (" + err.code + ")" + err.message;
		}		
	}
$$;