-- Stored Procedure to call change capture and change apply stored procedure for CDC Type2
-- Proc Name : SP_CDC_MAIN
-- Input Parameters
--      A. Source Table Name
--      B. Stage table Name
--		C. Target Table Name
--		D. Stream Name
--		E. Database Name
--		F. Source Schema Name
--      G. Target Schema Name
-- Author : Anup Mukhopadhyay (IBM Consultant) 

CREATE OR REPLACE PROCEDURE <deployment database name>.GTS_STG.SP_CDC_MAIN(SRC_TABLE VARCHAR, STG_TABLE VARCHAR, TGT_TABLE VARCHAR, STREAM_NAME VARCHAR, DB VARCHAR, SRC_SCHMA VARCHAR, TGT_SCHMA VARCHAR, SRC_FILE_TYPE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    var v_step=0 //define variables

    var return_rows1 = [];
    var return_rows2 = [];
    var return_rows3 = [];
	
	try {
	
		var snpst_dt_qry = `select distinct to_char(snpst_dt) from ${DB}.${SRC_SCHMA}.${SRC_TABLE};`
		var snpst_dt_stmt = snowflake.createStatement({sqlText: snpst_dt_qry}).execute();
		snpst_dt_stmt.next();
			var snpst_dt = snpst_dt_stmt.getColumnValue(1);
	}
    catch(err){
		return "Error in " + "step: " + v_step + ". Error messege: " + err;
    }

	try {
		v_step=10 // load date in log table
		
        var sql_cmd1 = `select to_char(current_timestamp()) as curr_time;`;
        var sql_stmt1 = snowflake.createStatement({sqlText: sql_cmd1});
        var result_set=sql_stmt1.execute();

        result_set.next();
          var load_ts = result_set.getColumnValue(1);
    }
	catch(err) {
		return ("Error in step: " + v_step + "; Messege :" + err);
	}	

    var v_step=20; // create Type2 merge statement for Staging table
    
    var stmt_qry = "call " + DB + "." + SRC_SCHMA + ".SP_CHANGE_CAPTURE('"+ SRC_TABLE +"','"+ STG_TABLE +"','"+ DB +"','"+ SRC_SCHMA +"');";
    var stmt = snowflake.createStatement({sqlText: stmt_qry});
    
        // Snowflake statement execution part
        try{
            var result1 = stmt.execute();
            result1.next();
            var merge_stmt1 = result1.getColumnValue(1);
        }
        
        // Exception handling part
        catch(err){
            return "Error in " + "step: " + v_step + ". Error messege: " + err;
        }

    var v_step=30; // execute Type2 merge statement for Staging table

    var stmt2 = snowflake.createStatement({sqlText: merge_stmt1});
        
        // Snowflake statement execution part
        try{
            var result2 = stmt2.execute();
            result2.next();
			stg_insert = result2.getColumnValue(1);
			stg_update = result2.getColumnValue(2);
			var audit_insert_into = snowflake.createStatement({sqlText:`insert into ${DB}.${SRC_SCHMA}.ing_px_audit_log
				   (src_file_name,src_file_type,schema_name,table_name,rows_parsed,rows_insert,rows_update,rows_delete,snapshot_dt,load_ts) VALUES (?,?,?,?,?,?,?,?,?,?);`
				   ,binds : ['NA','NA',SRC_SCHMA,STG_TABLE,0,stg_insert,stg_update,0,snpst_dt,load_ts]
			});
			audit_insert_into.execute();
            return_rows1.push(STG_TABLE + " -> Number of rows inserted: " + stg_insert + " / " + "Number of rows updated: " + stg_update);
        }
        // Exception handling part
        catch(err){
            return "Error in " + "step: " + v_step + ". Error messege: " + err;
        }
    
    // -- Load Type2 in Main table

if (SRC_FILE_TYPE == 'F') {

    var v_step=40; // create Type2 merge statement for core table

    var stmt3_qry = "call " + DB + "." + SRC_SCHMA + ".SP_CHANGE_APPLY('"+ STREAM_NAME +"','"+ TGT_SCHMA +"','"+ TGT_TABLE +"','"+ DB +"','"+ SRC_SCHMA +"','"+ SRC_TABLE +"');";
    var stmt3 = snowflake.createStatement({sqlText: stmt3_qry});
    
        // Snowflake statement execution part
        try{
            var result3 = stmt3.execute();
            result3.next();
            var merge_stmt2 = result3.getColumnValue(1);
        }
        
        // Exception handling part
        catch(err){
            return "Error in " + "step: " + v_step + ". Error messege: " + err;
        }
    
    var v_step=50; // execute Type2 merge statement for core table
    
    var stmt4 = snowflake.createStatement({sqlText: merge_stmt2});
    
        // Snowflake statement execution part
        try{
            var result4 = stmt4.execute();
            result4.next();
			trfn_insert = result4.getColumnValue(1);
			trfn_update = result4.getColumnValue(2);
			var audit_insert_into = snowflake.createStatement({sqlText:`insert into ${DB}.${SRC_SCHMA}.ing_px_audit_log
				   (src_file_name,src_file_type,schema_name,table_name,rows_parsed,rows_insert,rows_update,rows_delete,snapshot_dt,load_ts) VALUES (?,?,?,?,?,?,?,?,?,?);`
				   ,binds : ['NA','NA',TGT_SCHMA,TGT_TABLE,0,trfn_insert,trfn_update,0,snpst_dt,load_ts]
			});
			audit_insert_into.execute();			
            return_rows2.push(TGT_TABLE + " -> Number of rows inserted: " + result4.getColumnValue(1) + " / " + "Number of rows updated: " + result4.getColumnValue(2));
            //fin_return_col = return_rows1 + " : " + return_rows2;
        //return (fin_return_col)
        }
        
        // Exception handling part
        catch(err){
            return "Error in " + "step: " + v_step + ". Error messege: " + err;
        }

	var v_step=60; // create Type2 merge statement for delets in core table

		var stmt5_qry = "call " + DB + "." + SRC_SCHMA + ".SP_CHANGE_APPLY_DELETE('"+ DB +"','"+ SRC_SCHMA + "','" + TGT_SCHMA + "','" + SRC_TABLE +"','"+ TGT_TABLE +"');";        
		var stmt5 = snowflake.createStatement({sqlText: stmt5_qry});
		
		// Snowflake statement execution to create merge statement
		try{
			var result5 = stmt5.execute();
			result5.next();
			var merge_stmt3 = result5.getColumnValue(1);
		}
	
		// Exception handling part
		catch(err){
			return "Error in " + "step: " + v_step + ". Error messege: " + err;
		}
		
    var v_step=70; // execute Type2 merge statement for delets in core table

		var stmt6 = snowflake.createStatement({sqlText: merge_stmt3});
		
		// Snowflake statement execution part
		try{
			var result6 = stmt6.execute();
			result6.next();
			trfn_delete = result6.getColumnValue(1);
			var audit_insert_into = snowflake.createStatement({sqlText:`insert into ${DB}.${SRC_SCHMA}.ing_px_audit_log
				   (src_file_name,src_file_type,schema_name,table_name,rows_parsed,rows_insert,rows_update,rows_delete,snapshot_dt,load_ts) VALUES (?,?,?,?,?,?,?,?,?,?);`
				   ,binds : ['NA','NA',TGT_SCHMA,TGT_TABLE,0,trfn_insert,trfn_update,trfn_delete,snpst_dt,load_ts]
			});
			audit_insert_into.execute();
			return_rows3.push(TGT_TABLE + " -> Number of rows updated for source deletes: " + trfn_delete);
			fin_return_col = return_rows1 + " : " + return_rows2 + " : "+ return_rows3;
		return (fin_return_col);
		}
		
		// Exception handling part
		catch(err){
			return "Error in " + "step: " + v_step + ". Error messege: " + err;
		}
}
else {
    
    var v_step=80; // create Type2 merge statement for core table
    
    var stmt3_qry = "call " + DB + "." + SRC_SCHMA + ".SP_CHANGE_APPLY('"+ STREAM_NAME +"','"+ TGT_SCHMA +"','"+ TGT_TABLE +"','"+ DB +"','"+ SRC_SCHMA +"','"+ SRC_TABLE +"');";
    var stmt3 = snowflake.createStatement({sqlText: stmt3_qry});
    
        // Snowflake statement execution part to create the merge statement for change apply
        try{
            var result3 = stmt3.execute();
            result3.next();
            var merge_stmt2 = result3.getColumnValue(1);
        }
        
        // Exception handling part
        catch(err){
            return "Error in " + "step: " + v_step + ". Error messege: " + err;
        }
    
	var v_step=90; // execute Type2 merge statement for core table
    
    var stmt4 = snowflake.createStatement({sqlText: merge_stmt2});
		
		// Snowflake statement execution part to execute the merge statement for inserts/updates
		try{
			var result4 = stmt4.execute();
			result4.next();
			trfn_insert = result4.getColumnValue(1);
			trfn_update = result4.getColumnValue(2);	
			var audit_insert_into = snowflake.createStatement({sqlText:`insert into ${DB}.${SRC_SCHMA}.ing_px_audit_log
				   (src_file_name,src_file_type,schema_name,table_name,rows_parsed,rows_insert,rows_update,rows_delete,snapshot_dt,load_ts) VALUES (?,?,?,?,?,?,?,?,?,?);`
				   ,binds : ['NA','NA',TGT_SCHMA,TGT_TABLE,0,trfn_insert,trfn_update,0,snpst_dt,load_ts]
			});
			audit_insert_into.execute();			
			return_rows2.push(TGT_TABLE + " -> Number of rows inserted: " + result4.getColumnValue(1) + " / " + "Number of rows updated: " + result4.getColumnValue(2));
			fin_return_col = return_rows1 + " : " + return_rows2;
		return (fin_return_col)
		}
		
		// Exception handling part
		catch(err){
			return "Error in " + "step: " + v_step + ". Error messege: " + err;
		}
}
$$;