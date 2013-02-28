using ProbabilisticDatabase.Src.ControllerPackage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Data;

namespace ProbabilisticDatabase.Src.DatabaseEngine
{
    public class StandardDatabase : IStandardDatabase
    {
        public SqlConnection myConnection;

        public StandardDatabase()
        {
            // trace is just alternative to log4net, for logging purpose
            Trace.Listeners.Add(new TextWriterTraceListener(Console.Out));

            myConnection = new SqlConnection("user id=PDuser;" +
                                                   "password=1234888;server=localhost;" +
                                                   "Trusted_Connection=yes;" +
                                                   "database=ProbabilisticDatabase; " +
                                                   "connection timeout=30");
            try
            {
                myConnection.Open();
                myConnection.Close();
            }
            catch (System.Data.SqlClient.SqlException ex)
            {
                Console.WriteLine(ex.Message);
                throw new Exception("can not connect to database ");
            }
        }


        public bool checkIsTableAlreadyExist(string table)
        {

            string sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '" + table + "'";

            DataTable result = executeSQLWithResult(sql);
            return result.Rows.Count > 0;

        }

        public void createNewTable(string table, string[] attributeNames, string[] attributeTypes)
        {
            // example create sql: CREATE TABLE Dogs1 (Weight INT, Name TEXT, Breed TEXT)
            string attributeClause = "";
            for (int i = 0; i < attributeNames.Length; i++ )
            {
                attributeClause += attributeNames[i] + " " + attributeTypes[i] + " ";
                if(i != attributeNames.Length-1)
                {
                    attributeClause += ",";
                }

            }

            string sqlString = "CREATE TABLE " + table + " (" + attributeClause + ")";
            executeSQL(sqlString);

            Trace.WriteLine(sqlString);
        }

        public void insertValueIntoAttributeTable(string attributeTableName, int randomVariable, int value, string attribute, double prob)
        {
            // format for insert query : INSERT INTO table_name VALUES (value1, value2, value3,...)

            string sqlString = "INSERT INTO " + attributeTableName + 
                              " VALUES (" + randomVariable + "," + value + ",'" + attribute + "'," + prob + ")";
            executeSQL(sqlString);

            Trace.WriteLine("SQL executed : "+ sqlString);
        }

        /// <summary>
        /// return string is either the error message if error encountered, or success message
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        private string executeSQL(string sql)
        {
            try
            {
                myConnection.Open();
               
                using (SqlCommand command = new SqlCommand(
                sql, myConnection))
                {
                    command.ExecuteNonQuery();
                }

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return e.Message;
            }
            finally
            {
                myConnection.Close();
            }
            return "SQL executed successfully";
        }

        public DataTable executeSQLWithResult(string query)
        {
            try
            {
                // Open the connection
                myConnection.Open();

                // Create a SqlCommand object and pass the constructor the connection string and the query string.
                SqlCommand queryCommand = new SqlCommand(query, myConnection);

                // Use the above SqlCommand object to create a SqlDataReader object.
                SqlDataReader queryCommandReader = queryCommand.ExecuteReader();

                // Create a DataTable object to hold all the data returned by the query.
                DataTable dataTable = new DataTable();

                // Use the DataTable.Load(SqlDataReader) function to put the results of the query into a DataTable.
                dataTable.Load(queryCommandReader);

                return dataTable;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            finally
            {
                myConnection.Close();
            }

            return null;
        }

        /// <summary>
        /// return value of 0 means error occured
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public int getNextFreeVariableID(string tableName)
        {
            string primeTableName = tableName + "_0";
            string query = "select top 1 var+1 as newID from "+primeTableName
                + " where var + 1 not in (select var from " + primeTableName + ") order by var";
            DataTable result = executeSQLWithResult(query);

            if (result == null || result.Rows.Count < 1)
            {
                return 0;
            }else{
                Object newId = result.Rows[0]["newID"];
                if (newId is int)
                {
                    return (int)newId;
                }
                return 0;
            }

        }
    }
}
