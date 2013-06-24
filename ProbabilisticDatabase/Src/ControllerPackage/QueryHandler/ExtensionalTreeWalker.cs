using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;
using ProbabilisticDatabase.Src.ControllerPackage.Query.SelectQuery;

namespace ProbabilisticDatabase.Src.ControllerPackage.QueryHandler
{
    public class ExtensionalTreeWalker
    {
        private QueryTree _queryTree;
        private IStandardDatabase underlineDatabase;

        Func<string, string> prefixR1 = str => "R1." + str;
        Func<string, string> prefixR2 = str => "R2." + str;
        private Func<string, string> afterRenaming = str => GetStringAfterAs(str);
        private Func<string, string> beforeRenaming = str => GetStringBeforeAs(str);

        private static string GetStringAfterAs(string str)
        {
            string sPattern = @"\bas\s+(?<att>[\d\w]+)";
            Match match = Regex.Match(str, sPattern, RegexOptions.IgnoreCase);

            if (match.Success)
            {
                var att= match.Groups["att"].Value;
                return att;
            }
            return GetStringBeforeAs(str);
        }

        private static string GetStringBeforeAs(string str)
        {
            string sPattern = @"\A\s*(?<att>[\d\w]+)";
            Match match = Regex.Match(str, sPattern, RegexOptions.IgnoreCase);

            if (match.Success)
            {
                var att = match.Groups["att"].Value;
                return att;
            }
            throw new Exception("renaming function regex fail");
        }

        public ExtensionalTreeWalker(QueryTree queryTree, IStandardDatabase underlineDatabase)
        {
            this.underlineDatabase = underlineDatabase;
            _queryTree = queryTree;
        }

        public string GetSql()
        {
            List<string> attributes;
            return GetSql(_queryTree, out attributes);
        }

        public string GetSql(QueryTree queryTree, out List<string> atts)
        {
            List<QueryTree> subquery;
            string sql;
            List<string> AttsSelectedByQuery;
            switch (queryTree.treeNodeType)
            {
                case QueryTree.TreeNodeType.Select:
                    // If node is select condition, then Select * From H(query.Subquey[0]) Where query.condition
                    subquery = queryTree.subquery;
                    if ( subquery != null && subquery.Count==1)
                    {
                        var R1 = subquery[0];
                        List<string> attributes;
                        var fromClause = GetSql(R1, out attributes);
                        var condition = queryTree.condition;
                        sql = string.Format("select * from ({0}) as R where {1}", fromClause, condition);
                        atts = attributes;
                        return sql;
                    }
                    throw new Exception("number of subquery mismatch");
                case QueryTree.TreeNodeType.Project:
                    // If node is project A, then Select R.A,IndepProject(R.P) as p from R group by R.A
                    subquery = queryTree.subquery;
                    if (subquery != null && subquery.Count == 1)
                    {
                        var R1 = subquery[0];
                        if(R1.treeNodeType==QueryTree.TreeNodeType.GroundTable)
                        {
                            queryTree.treeNodeType=QueryTree.TreeNodeType.GroundTable;
                            queryTree.tableName = R1.tableName;
                            queryTree.subquery = new List<QueryTree>();
                            return GetSql(queryTree,out atts);
                        }
                        List<string> attributes;
                        var fromClause = GetSql(R1, out attributes);
                        AttsSelectedByQuery = _queryTree.attributes;
                        sql = string.Format("Select {0},dbo.IndependentProject(R.P) as p from ({1}) as R group by {0}", string.Join(",", AttsSelectedByQuery), fromClause);
                        atts = AttsSelectedByQuery;
                        return sql;
                    }
                    throw new Exception("number of subquery mismatch");
                case QueryTree.TreeNodeType.Join:
                    // If node is project A, then Select R.A,IndepProject(R.P) as p from R group by R.A
                    subquery = queryTree.subquery;
                    if (subquery != null && subquery.Count == 2)
                    {
                        List<string> a1;
                        List<string> a2;
                        var R1 = GetSql(subquery[0],out a1);
                        var R2 = GetSql(subquery[1],out a2);
                        var joinCondition=queryTree.condition;

                        var r1 = a1.Select(prefixR1);
                        var r2 = a2.Select(prefixR2);
                        var R1Atts = String.Join(",", r1);
                        var R2Atts = String.Join(",", r2);

                        sql = string.Format("Select {0},{1},(R1.p*R2.p/100) as p "+
                                            "from ({2}) as R1 "+
                                            "join ({3}) as R2 on {4}", R1Atts, R2Atts, R1, R2,joinCondition);
                        atts = (r1.Concat(r2)).ToList();
                        return sql;
                    }
                    throw new Exception("number of subquery mismatch");
                case QueryTree.TreeNodeType.Union:
                    subquery = queryTree.subquery;
                    if (subquery != null && subquery.Count == 2)
                    {
                        List<string> a1;
                        List<string> a2;
                        var R1 = GetSql(subquery[0], out a1);
                        var R2 = GetSql(subquery[1], out a2);
                        var R1Atts = String.Join(",", a1);
                        var R2Atts = String.Join(",", a2);

                        sql = string.Format("select coalesce(R1.{0}, R2.{3}), " +
                                            "(case when R1.p is null then R2.p " +
                                            "when R2.p is null then R1.p "+
                                            "else 100-(100-R1.p)*(100-R2.p)/100 "+
                                            "end) as p from "+
                                            "({1}) as R1 full outer join "+
                                            "({2}) as R2 on R1.{0} = R2.{0}", R1Atts, R1, R2, R2Atts);
                        atts = a2;
                        return sql;
                    }
                    throw new Exception("number of subquery mismatch");
                case QueryTree.TreeNodeType.Difference:
                    subquery = queryTree.subquery;
                    if (subquery != null && subquery.Count == 2)
                    {
                        List<string> a1;
                        List<string> a2;
                        var R1 = GetSql(subquery[0], out a1);
                        var R2 = GetSql(subquery[1], out a2);

                        var r1 = a1.Select(prefixR1);
                        var r2 = a2.Select(prefixR2);
                        var R1Atts = String.Join(",", r1);
                        var R2Atts = String.Join(",", r2);

                        sql = string.Format("Select {2},(R1.p*R2.p/100) as p " +
                                            "from ({0}) as R1 " +
                                            "join (Select {4}, 100-R.P as P from ({1}) as R) as R2 on {2}={3}", R1, R2, R1Atts, R2Atts, String.Join(",", a2));
                        atts= a1;
                        return sql;
                    }
                    throw new Exception("number of subquery mismatch");

                case QueryTree.TreeNodeType.GroundTable:
                    var tName = queryTree.tableName;
                    var stateTable = tName + "_PossibleStates";
                    underlineDatabase.DropTableIfExist(stateTable);
                    var allAttsFromTable = PreparePossibleStatesTable(tName);
                    AttsSelectedByQuery = queryTree.attributes;
                    if ( AttsSelectedByQuery==null || (AttsSelectedByQuery.Count==1 && AttsSelectedByQuery[0]=="*"))
                    {
                        AttsSelectedByQuery = allAttsFromTable;
                    }
                    var attBeforeRenaming = AttsSelectedByQuery.Select(beforeRenaming).ToList();
                    var attAfterRenaming = AttsSelectedByQuery.Select(afterRenaming).ToList();
                    sql = string.Format("select var,{1},Sum(p) as p from {0} Group By var,{2}", stateTable, string.Join(",", AttsSelectedByQuery), string.Join(",", attBeforeRenaming));
                    var finalSql = string.Format("select {1},dbo.IndependentProject(p) as p from ({0}) as R Group By {1}", sql, string.Join(",", attAfterRenaming));
                    atts = attAfterRenaming;
                    return finalSql;
                        
                default:
                    throw new Exception("tree node type is invalid");
            }
            return "";
        }

        /// <summary>
        /// construct and execute sql to populate possible states table using those attribute table. 
        /// </summary>
        /// <param name="tableName"></param>
        private List<string> PreparePossibleStatesTable(string tableName)
        {
            if (!underlineDatabase.CheckIsTableAlreadyExist(tableName + "_0"))
            {
                throw new Exception(tableName + "_0 table doesn't exist");
            }
            List<List<string>> colNamesList = new List<List<string>>();
            var getVariables = string.Format("SELECT var,att0,p FROM {0}_0", tableName);
            var getVariblesResult = underlineDatabase.ExecuteSqlWithResult(getVariables);
            foreach (DataRow row in getVariblesResult.Rows)
            {
                var variable = row.Field<int>("var");
                var tableNameString = row.Field<string>("att0");
                var tableNames = new List<string>(tableNameString.Split(','));
                var p = row.Field<double>("p");
                List<string> colNames = GeneratePossibleStates(tableName, variable, tableNames, p);
                colNamesList.Add(colNames);
            }

            if ((colNamesList.Count) > 1)
            {
                return colNamesList[0];
            }
            else
            {
                throw new Exception("inconsistency in attribute sizes of different tuples");
            }
        }

        /// <summary>
        /// return number of attributes implied by tableNames
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="variable"></param>
        /// <param name="tableNames"></param>
        /// <param name="p"></param>
        /// <returns></returns>
        private List<string> GeneratePossibleStates(string tableName, int variable, List<string> tableNames, double p)
        {
            List<string> colNameList = new List<string>();
            string selectClause = "t0.var,";
            string probabilityClause = "100*(t0.p/100)";
            string joinClause = tableName + "_0 t0";
            List<string> emptyString = new List<string>();
            var colIndex = 1;
            for (int i = 0; i < tableNames.Count; i++)
            {
                var tName = tableNames[i];
                var aProbability = string.Format("*({0}.p/100)", tName);
                probabilityClause += aProbability;
                var aJoin = string.Format(" JOIN {0} ON t0.var = {0}.var", tName);
                joinClause += aJoin;
                int numberOfDigits = NumberOfDigits(tName);
                for (int j = 0; j < numberOfDigits; j++)
                {
                    selectClause += "att" + (colIndex) + ",";
                    colNameList.Add("att" + (colIndex));
                    emptyString.Add("null");
                    colIndex++;
                }
            }

            var statesTable = tableName + "_PossibleStates";
            var tableAlreadyExist = underlineDatabase.CheckIsTableAlreadyExist(statesTable);
            if (tableAlreadyExist)
            {
                var sql = string.Format("INSERT INTO {2} SELECT {0}{1} as p FROM {3} WHERE t0.var={4}",
                    selectClause, probabilityClause, statesTable, joinClause, variable);
                underlineDatabase.ExecuteSql(sql);
            }
            else
            {
                var sql = string.Format("SELECT {0}{1} as p INTO {2} FROM {3} WHERE t0.var={4}",
                    selectClause, probabilityClause, statesTable, joinClause, variable);
                underlineDatabase.ExecuteSql(sql);
            }

            if ((int)Math.Round(p) < 100)
            {
                var sql = string.Format("INSERT INTO {0} VALUES ({2},{1},{3})",
                    statesTable, string.Join(",", emptyString), variable, p);
                underlineDatabase.ExecuteSql(sql);
            }
            return colNameList;
        }

        private int NumberOfDigits(string tableName)
        {
            const string sPattern = @"(?<digit>\d)";
            var matches = Regex.Matches(tableName, sPattern, RegexOptions.IgnoreCase);
            return matches.Count;
        }

    }
}