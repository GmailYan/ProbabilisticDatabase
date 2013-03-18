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
        private readonly SqlConnection _myConnection;

        public StandardDatabase()
        {
            // trace is just alternative to log4net, for logging purpose
            Trace.Listeners.Add(new TextWriterTraceListener(Console.Out));

            

            string connectionString2 = "user id=PDuser;" +
                                                   "password=1234888;server=localhost;" +
                                                   "Trusted_Connection=yes;" +
                                                   "database=ProbabilisticDatabase; " +
                                                   "connection timeout=30";

            //string connectionString = "Data Source=192.168.1.205,1433;Network Library=DBMSSOCN;Initial Catalog=ProbabilisticDatabase;User ID=PDuser;Password=1234888;";
            const string connectionString = "Data Source=86.1.79.91,1433;Network Library=DBMSSOCN;Initial Catalog=ProbabilisticDatabase;User ID=PDuser;Password=1234888;";
            _myConnection = new SqlConnection(connectionString2);
            try
            {
                _myConnection.Open();
                _myConnection.Close();
            }
            catch (SqlException ex)
            {
                Console.WriteLine(ex.Message);
                throw new Exception("can not connect to database ");
            }
        }


        public bool CheckIsTableAlreadyExist(string table)
        {

            string sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '" + table + "'";

            DataTable result = ExecuteSqlWithResult(sql);
            return result.Rows.Count > 0;

        }

        public void CreateNewTable(string table, string[] attributeNames, string[] attributeTypes)
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
            ExecuteSql(sqlString);

            Trace.WriteLine(sqlString);
        }

        public void InsertValueIntoAttributeTable(string attributeTableName, int randomVariable, int value, string attribute, double prob)
        {
            // format for insert query : INSERT INTO table_name VALUES (value1, value2, value3,...)

            string sqlString = "INSERT INTO " + attributeTableName + 
                              " VALUES (" + randomVariable + "," + value + ",'" + attribute + "'," + prob + ")";
            ExecuteSql(sqlString);

            Trace.WriteLine("SQL executed : "+ sqlString);
        }

        /// <summary>
        /// return string is either the error message if error encountered, or success message
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public string ExecuteSql(string sql)
        {
            try
            {
                _myConnection.Open();
               
                using (SqlCommand command = new SqlCommand(
                sql, _myConnection))
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
                _myConnection.Close();
            }
            return "SQL executed successfully";
        }

        public void WriteTableBacktoDatabase(string tableName, DataTable result)
        {

            try
            {
                _myConnection.Open();
                var adapter = new SqlDataAdapter("SELECT * FROM " + tableName, _myConnection);
                using (new SqlCommandBuilder(adapter))
                {
                    //
                    // Fill the DataAdapter with the values in the DataTable.
                    //
                    adapter.Fill(result);
                    //
                    // Insert the data table into the SQL database.
                    //
                    adapter.Update(result);
                }

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            finally
            {
                _myConnection.Close();
            }


            
        }

        public DataTable ExecuteSqlWithResult(string query)
        {
            try
            {
                // Open the connection
                _myConnection.Open();

                // Create a SqlCommand object and pass the constructor the connection string and the query string.
                SqlCommand queryCommand = new SqlCommand(query, _myConnection);

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
                _myConnection.Close();
            }

            return null;
        }

        public int GetNumberOfPossibleWorlds(string tableName)
        {
            if (CheckIsTableAlreadyExist(tableName+"_PossibleWorlds"))
            {
                string sql = string.Format("select top 1 WorldNo from {0}_PossibleWorlds order by WorldNo desc", tableName);
                var result = ExecuteSqlWithResult(sql);
                var top = result.Rows[0]["WorldNo"];
                if (!(top is int))
                {
                    return 0;
                }
                return (int) top;
            }
            return 0;
        }

        /// <summary>
        /// return value of 0 means error occured
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public int GetNextFreeVariableId(string tableName)
        {
            string primeTableName = tableName + "_0";
            string query = "select top 1 var+1 as newID from "+primeTableName
                + " where var + 1 not in (select var from " + primeTableName + ") order by var";
            DataTable result = ExecuteSqlWithResult(query);

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

        public void DropTableIfExist(string tableName)
        {
            if (CheckIsTableAlreadyExist(tableName))
            {
                ExecuteSql("DROP TABLE " + tableName);    
            }
            
        }
    }
}
