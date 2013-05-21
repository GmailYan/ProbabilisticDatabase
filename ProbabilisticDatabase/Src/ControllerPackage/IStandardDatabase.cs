using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace ProbabilisticDatabase.Src.ControllerPackage
{
    interface IStandardDatabase
    {
        bool CheckIsTableAlreadyExist(string table);

        void CreateNewTable(string table, string[] attributeNames, string[] attributeTypes);

        void InsertValueIntoAttributeTable(string attributeTableName, int randomVariable, int p1, string p2, double p3);

        int GetNextFreeVariableId(string tableName);

        DataTable ExecuteSqlWithResult(string sql);

        int GetNumberOfPossibleWorlds(string tableName);

        string ExecuteSql(string sql);
        void WriteTableBacktoDatabase(string tableName, DataTable result);
        void DropTableIfExist(string tableName);
    }
}
