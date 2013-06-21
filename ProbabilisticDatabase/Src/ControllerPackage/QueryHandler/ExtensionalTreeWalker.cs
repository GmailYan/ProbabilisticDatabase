using System;
using System.Collections.Generic;
using ProbabilisticDatabase.Src.ControllerPackage.Query.SelectQuery;

namespace ProbabilisticDatabase.Src.ControllerPackage.QueryHandler
{
    public class ExtensionalTreeWalker
    {
        private QueryTree _queryTree;
        public ExtensionalTreeWalker(QueryTree queryTree)
        {
            _queryTree = queryTree;
        }

        public string GetSql()
        {
            string attributes;
            return GetSql(_queryTree, out attributes);
        }

        public string GetSql(QueryTree queryTree, out string atts)
        {
            List<QueryTree> subquery;
            string sql;
            switch (queryTree.treeNodeType)
            {
                case QueryTree.TreeNodeType.Select:
                    // If node is select condition, then Select * From H(query.Subquey[0]) Where query.condition
                    subquery = queryTree.subquery;
                    if ( subquery != null && subquery.Count==1)
                    {
                        var R1 = subquery[0];
                        string attributes;
                        var fromClause = GetSql(R1, out attributes);
                        var condition = queryTree.condition;
                        sql = string.Format("select * from {0} where {1}", fromClause, condition);
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
                        string attributes;
                        var fromClause = GetSql(R1, out attributes);
                        var A = _queryTree.attributes;
                        sql = string.Format("Select R.{0},IndepProject(R.P) as p from {1} group by R.{0}", A, fromClause);
                        atts = A;
                        return sql;
                    }
                    throw new Exception("number of subquery mismatch");
                case QueryTree.TreeNodeType.Join:
                    // If node is project A, then Select R.A,IndepProject(R.P) as p from R group by R.A
                    subquery = queryTree.subquery;
                    if (subquery != null && subquery.Count == 2)
                    {
                        string A1;
                        string A2;
                        var R1 = GetSql(subquery[0],out A1);
                        var R2 = GetSql(subquery[1],out A2);
                        var joinCondition=queryTree.condition;
                        sql = string.Format("Select R1.{0},R2.{4},R1.p*R2.p as p"+
                                            "from {1} as R1"+
                                            "join {2} as R2 on {3}", A1, R1, R2, joinCondition,A2);
                        atts = A1 + "," + A2;
                        return sql;
                    }
                    throw new Exception("number of subquery mismatch");
                case QueryTree.TreeNodeType.Union:
                    subquery = queryTree.subquery;
                    if (subquery != null && subquery.Count == 1)
                    {
                        string a2;
                        var R1 = GetSql(subquery[0], out a2);
                        var R2 = GetSql(subquery[1], out a2);
                        var A = queryTree.attributes;

                        sql = string.Format("select coalesce(R1.{0}, R2.{0})," +
                                            "(case when R1.p is null then R2.p" +
                                            "when R2.p is null then R1.p"+
                                            "else 100 - (100-R1.p)*(100-R2.p)"+
                                            "end) as p from"+
                                            "{1} as R1 full outer join"+
                                            "{2} as R2 on R1.{0} = R2.{0};", A, R1, R2);
                        atts = a2;
                        return sql;
                    }
                    throw new Exception("number of subquery mismatch");
                case QueryTree.TreeNodeType.Difference:
                    subquery = queryTree.subquery;
                    if (subquery != null && subquery.Count == 2)
                    {
                        string a2;
                        var R1 = GetSql(subquery[0], out a2);
                        var R2 = GetSql(subquery[1], out a2);
                        var A = queryTree.attributes;

                        sql = string.Format("Select R1.{0},R2.{0},R1.p*R2.p as p" +
                                            "from {1} as R1" +
                                            "join (Select R.{0}, 100-R.P as P from {2} as R)", A, R1, R2);
                        atts= a2;
                        return sql;
                    }
                    throw new Exception("number of subquery mismatch");

                case QueryTree.TreeNodeType.GroundTable:
                    var tName = queryTree.tableName;
                    var stateTable = tName + "_PossibleStates";
                    atts = PrepareStatesTable(stateTable);
                    sql = string.Format("select {1},Sum(p) as p from {0} Group By {1}", stateTable, atts);
                    return sql;
                        
                default:
                    throw new Exception("tree node type is invalid");
            }
            return "";
        }

        private string PrepareStatesTable(string stateTable)
        {
            throw new NotImplementedException();
        }
    }
}