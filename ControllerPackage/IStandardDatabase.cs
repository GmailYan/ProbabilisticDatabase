using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ProbabilisticDatabase.Src.ControllerPackage
{
    interface IStandardDatabase
    {
        bool checkIsTableAlreadyExist(string table);

        void createNewTable(string table, string[] attributeNames, string[] attributeTypes);

        void insertValueIntoAttributeTable(string attributeTableName, int randomVariable, int p1, string p2, double p3);
    }
}
