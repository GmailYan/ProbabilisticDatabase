using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ProbabilisticDatabase.Src.ControllerPackage.Query;

namespace PDtests
{
    [TestClass]
    public class GeneralSQLParserTest
    {

        [TestMethod]
        public void Test1()
        {
            GeneralSQLParser parser = new GeneralSQLParser();
            var sql = "select * from ((select a from b on condition) union (select x from b)) union ((select a from b on condition) union (select x from b)) where a=c";
            parser.Parse(sql);
        }

        [TestMethod]
        public void Test2()
        {
            GeneralSQLParser parser = new GeneralSQLParser();
            var sql = "select * from (Select a from table2) as R2 join table1 as R1 on R1.a=R2.a";
            parser.Parse(sql);
        }

    }
}
