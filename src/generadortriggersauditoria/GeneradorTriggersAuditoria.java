/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package generadortriggersauditoria;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Meltsan
 */
public class GeneradorTriggersAuditoria {

    private static Connection conn;
    private static String host = "jdbc:postgresql://dev-admin.meltsan.com:5432/";
    private static String db = "mr_admin";
    private static String user = "postgres";
    private static String password = "";
    private static String tableName = "mr_user_doctors";
    private static String sqlQuery;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, SQLException {
        //Conexion a la base de datos                                  

        //String[] tablasAuditar = {tableName};
        String[] tablasAuditar = {"mr_patient_medicines", "mr_doctor_attrvalues", "mr_doctor_calendar", "mr_hd_agenda_sessions",
            "mr_hd_clinical_summaries", "mr_hd_procedures", "mr_medical_agenda", "mr_notification_details",
            "mr_catalogue_attributes",
            "mr_catalogue_structures", "mr_catalogues", "mr_catalogues_values", "mr_hospital_contracts",
            "mr_laboratory_attributes", "mr_medical_notes", "mr_processes", "mr_system_parameters",
            "mr_users", "mr_laboratory_attrvalues", "mr_notes_files",
            "mr_patient_laboratories", "mr_clinical_records", "mr_contract_attrvalues", "mr_hd_data_sessions",
            "mr_hd_materials", "mr_hd_medicines", "mr_hist_user_patients", "mr_patient_attrvalues",
            "mr_patient_contacts", "mr_patient_vaccine", "mr_rel_doctors_hospitals",
            "mr_hospitals", "mr_note_sections", "mr_notes_attributes", "mr_procnotifications",
            "mr_rol_screens", "mr_roles", "mr_screens", "mr_user_doctors",
            "mr_record_laboratories", "mr_hd_symptoms", "mr_hd_patient_sessions", "mr_user_patients"
        };

        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(host + db, user, password);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(GeneradorTriggersAuditoria.class.getName()).log(Level.SEVERE, null, ex);
        }
        //Ejecutar la primera vez para hacer pruebas de inserccion sin restricciones
        //borrarConstraints(conn,tablasAuditar);

        for (String table : tablasAuditar) {
            System.out.println("tabla a auditar : " + table);
            sqlQuery = "Select * FROM mr_admin_05052017." + table + " LIMIT 1";
            try {
                ResultSet rs = conn.createStatement().executeQuery(sqlQuery);
                ResultSetMetaData rsmd = rs.getMetaData();
                escribirArchivo(table, rsmd);

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void borrarConstraints(Connection conn, String[] tablasAuditar) throws SQLException {
        String sqlQuery;
        String sqlQueryDropC;
        ResultSet rs;
        for (String tableName : tablasAuditar) {
            sqlQuery = "SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE\n"
                    + "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS\n"
                    + "WHERE TABLE_NAME = '" + tableName + "' \n"
                    + "    and CONSTRAINT_SCHEMA = 'mr_admin_05052017' \n"
                    + "    and CONSTRAINT_SCHEMA = 'mr_admin_05052017' \n"
                    + "    and CONSTRAINT_TYPE = 'FOREIGN KEY'";
            rs = conn.createStatement().executeQuery(sqlQuery);
            System.out.println("CONSTRAINS DE LA TABLA: " + tableName);
            while (rs.next()) {
                sqlQueryDropC = " alter table mr_admin_05052017." + tableName + "\n"
                        + "  drop constraint " + rs.getString(1) + ";";
                int r = conn.createStatement().executeUpdate(sqlQueryDropC);
                if (r != 1) {
                    System.out.println(rs.getString(1) + " ** Elimanda correctamente");
                }
            }
            System.out.println("-----------------------");
        }
    }

    public static void escribirArchivo(String tableName, ResultSetMetaData rsmd) throws IOException, SQLException {
        String ruta = "C:\\Users\\VICENTE\\Desktop\\MedicalRecord\\Triggers\\TRIGGER_" + tableName + ".sql";
        File archivo = new File(ruta);
        BufferedWriter bw;
        StringBuilder tg = new StringBuilder();
        bw = new BufferedWriter(new FileWriter(archivo));
        int cols = rsmd.getColumnCount();
        System.out.printf("The query fetched %d columns\n", cols);
        tg.append("-- Function: public.trg_all_" + tableName + "()\n"
                + "\n"
                + "--DROP FUNCTION public.trg_all_" + tableName + "();\n\n");
        tg.append("CREATE OR REPLACE FUNCTION public.trg_all_" + tableName + "()\n"
                + "  RETURNS trigger AS\n"
                + "$BODY$\n"
                + "DECLARE \n"
                + "seq bigint; ds_table varchar  (500);\n"
                + "yr bigint;\n"
                + "BEGIN\n"
                + "  --obtiene las tablas indicadas para ser auditadas\n"
                + "  ds_table := (select B.attribute_value_01\n"
                + "		  FROM mr_admin_05052017.mr_catalogues A, mr_admin_05052017.mr_catalogues_values B\n"
                + "		  where A.id_catalogue = B.id_catalogue and \n"
                + "		  B.attribute_value_02 ='S' and\n"
                + "		  B.attribute_value_01 in (select attribute_value_01 \n"
                + "					from mr_admin_05052017.mr_catalogues_values\n"
                + "					where attribute_value_01 like '%TG_TABLE_NAME%')\n"
                + "		);\n"
                + "  ds_table := ('ds_table');\n"
                + "\n"
                + " IF (ds_table is not NULL AND ds_table <>' ') THEN\n");
        tg.append("\n	seq:= (select nextval('mr_admin_05052017.seq_mr_audit_tables'));\n"
                + "	yr := (cast((to_char(NOW()::date,'YYYY') ) as integer));");
        tg.append("\n	IF (TG_OP = 'INSERT') THEN\n"
                + "			INSERT INTO mr_admin_05052017.mr_audit_tables(cd_year, cd_table, cd_status, ds_table, user_updated,date_updated, user_inserted, date_inserted)\n"
                + "			VALUES (yr, TG_TABLE_NAME,'A', TG_TABLE_NAME, NULL,NULL,current_user,now());");

        for (int i = 1; i <= cols; i++) {
            String colName = rsmd.getColumnName(i);
            String colType = rsmd.getColumnTypeName(i);
            if ("varchar".equals(colType)) {
                tg.append("\n		 IF (NEW." + colName + " is  not null AND NEW." + colName + " <>'') THEN\n");
            } else {
                tg.append("\n		 IF (NEW." + colName + " is not NULL) THEN\n");
            }
            tg.append("\n"
                    + "			INSERT INTO mr_admin_05052017.mr_audit_details(cd_year, id_transaction, cd_table, regid, user_audit, cd_transaction_type, date_hour, current_value, previous_value, \n"
                    + "			column_name, user_updated, date_updated, user_inserted, date_inserted)\n"
                    + "			VALUES (yr, seq, TG_TABLE_NAME, TG_RELID, current_user, 'I', now(), NEW." + colName + ", NULL, '" + colName + "', NULL, NULL, current_user, NOW());\n"
                    + "		  END IF;");
        }
        tg.append("\n	RETURN NEW;\n"
                + "	-------------------------------------------------------------------------------------------\n"
                + "	--DELETE\n"
                + "	-------------------------------------------------------------------------------------------\n"
                + "	ELSIF (TG_OP = 'DELETE') THEN\n"
                + "--- DELETE\n"
                + "			INSERT INTO mr_admin_05052017.mr_audit_tables(cd_year, cd_table, cd_status, ds_table, user_updated,date_updated, user_inserted, date_inserted)\n"
                + "			VALUES (yr, TG_TABLE_NAME,'A', TG_TABLE_NAME, current_user,now(),current_user,now());");

        for (int i = 1; i <= cols; i++) {
            String colName = rsmd.getColumnName(i);
            String colType = rsmd.getColumnTypeName(i);
            if ("varchar".equals(colType)) {
                tg.append("\n		 IF (OLD." + colName + " is  not null AND OLD." + colName + " <>'') THEN\n");
            } else {
                tg.append("\n		 IF (OLD." + colName + " is not NULL) THEN\n");
            }
            tg.append("\n"
                    + "			INSERT INTO mr_admin_05052017.mr_audit_details(cd_year, id_transaction, cd_table, regid, user_audit, cd_transaction_type, date_hour, current_value, previous_value, \n"
                    + "			column_name, user_updated, date_updated, user_inserted, date_inserted)\n"
                    + "			VALUES (yr,seq,  TG_TABLE_NAME, TG_RELID, current_user, 'D', now(), NULL, OLD." + colName + ", '" + colName + "' , current_user, NOW(), current_user, NOW());	\n"
                    + "		\n"
                    + "		END IF;");
        }
        tg.append("	RETURN OLD;\n"
                + "	-------------------------------------------------------------------------------------------\n"
                + "	--UPDATE\n"
                + "	-------------------------------------------------------------------------------------------\n"
                + "	ELSIF  (TG_OP = 'UPDATE') THEN\n"
                + "			INSERT INTO mr_admin_05052017.mr_audit_tables(cd_year, cd_table, cd_status, ds_table, user_updated,date_updated, user_inserted, date_inserted)\n"
                + "			VALUES (yr, TG_TABLE_NAME,'A', TG_TABLE_NAME, current_user,now(),current_user,now());\n\n");

        for (int i = 1; i <= cols; i++) {
            String colName = rsmd.getColumnName(i);
            String colType = rsmd.getColumnTypeName(i);
            tg.append("		IF ((NEW." + colName + " <> OLD." + colName + ")  OR (NEW." + colName + " is not NULL and OLD." + colName + " is NULL)) THEN\n"
                    + "			\n"
                    + "			INSERT INTO mr_admin_05052017.mr_audit_details(cd_year, id_transaction, cd_table, regid, user_audit, cd_transaction_type, date_hour, current_value, previous_value, \n"
                    + "			column_name, user_updated, date_updated, user_inserted, date_inserted)\n"
                    + "			VALUES (yr,seq,  TG_TABLE_NAME, TG_RELID, current_user, 'U', now(), NEW." + colName + ", OLD." + colName + ", '" + colName + "' , current_user, NOW(), current_user, NOW());\n"
                    + "		 END IF;\n");
        }
        tg.append("	RETURN NEW;\n"
                + "	END IF;\n"
                + "END IF;\n"
                + "RETURN NULL;\n"
                + "END;\n"
                + "$BODY$\n"
                + "  LANGUAGE plpgsql VOLATILE\n"
                + "  COST 100;\n"
                + "ALTER FUNCTION public.trg_all_" + tableName + "()\n"
                + "  OWNER TO postgres;");

        tg.append("\n\n\n\n\n");
        //Creando las llamadas al TRIGGER de DELETE desde la Tabla
        tg.append("-- Trigger: trg_all_" + tableName + "_d on mr_admin_05052017." + tableName + "\n"
                + "\n"
                + "DROP TRIGGER IF EXISTS trg_all_" + tableName + "_d ON mr_admin_05052017." + tableName + ";\n"
                + "\n"
                + "CREATE TRIGGER trg_all_" + tableName + "_d\n"
                + "  AFTER DELETE\n"
                + "  ON mr_admin_05052017." + tableName + "\n"
                + "  FOR EACH ROW\n"
                + "  EXECUTE PROCEDURE public.trg_all_" + tableName + "();");

        tg.append("\n\n\n");
        //Creando las llamadas al TRIGGER de INSERT desde la Tabla
        tg.append("-- Trigger: trg_all_" + tableName + "_i on mr_admin_05052017." + tableName + "\n"
                + "\n"
                + "DROP TRIGGER IF EXISTS trg_all_" + tableName + "_i ON mr_admin_05052017." + tableName + ";\n"
                + "\n"
                + "CREATE TRIGGER trg_all_" + tableName + "_i\n"
                + "  AFTER INSERT\n"
                + "  ON mr_admin_05052017." + tableName + "\n"
                + "  FOR EACH ROW\n"
                + "  EXECUTE PROCEDURE public.trg_all_" + tableName + "();");

        tg.append("\n\n\n");
        //Creando las llamadas al TRIGGER de UPDATE desde la Tabla
        tg.append("-- Trigger: trg_all_" + tableName + "_u on mr_admin_05052017." + tableName + "\n"
                + "\n"
                + "DROP TRIGGER IF EXISTS trg_all_" + tableName + "_u ON mr_admin_05052017." + tableName + ";\n"
                + "\n"
                + "CREATE TRIGGER trg_all_" + tableName + "_u\n"
                + "  AFTER UPDATE\n"
                + "  ON mr_admin_05052017." + tableName + "\n"
                + "  FOR EACH ROW\n"
                + "  EXECUTE PROCEDURE public.trg_all_" + tableName + "();");
        bw.write(tg.toString());
        bw.close();
        ejecutarTrigger(tg.toString());
    }

    public static void ejecutarTrigger(String tg) throws SQLException {
        int r = conn.createStatement().executeUpdate(tg);
        if (r != 1) {
            System.out.println("TRIGGER LANZADO CORRECTAMENTE");
        }
    }
}
