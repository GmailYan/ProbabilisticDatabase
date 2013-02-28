using ProbabilisticDatabase.Src.ControllerPackage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Diagnostics;

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
            
            try
            {
                myConnection.Open();
                int NoOfSelected = 0;

                using (SqlCommand command = new SqlCommand(
                sql, myConnection))
                {
                    NoOfSelected = command.ExecuteNonQuery();
                }
                return NoOfSelected>0;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            finally
            {
                myConnection.Close();
            }

            return false;

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
                              " VALUES (" + randomVariable + "," + value + "," + attribute + "," + prob + ")";
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

    }
}
