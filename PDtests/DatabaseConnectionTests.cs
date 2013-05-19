using Microsoft.VisualStudio.TestTools.UnitTesting;
using ProbabilisticDatabase.Src.DatabaseEngine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PDtests
{
    [TestClass]
    public class DatabaseConnectionTests
    {
        [TestMethod]
        public void TestConnectionOpen()
        {
            StandardDatabase db = new StandardDatabase();

        }
    }
}
