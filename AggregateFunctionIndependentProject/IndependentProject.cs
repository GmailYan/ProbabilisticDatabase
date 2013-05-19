
using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

[Serializable]
[Microsoft.SqlServer.Server.SqlUserDefinedAggregate(
    Format.Native,
    Name = "IndependentProject"                 // name of the aggregate
    )]
public struct IndependentProject
{
    public System.Data.SqlTypes.SqlDouble complementProduct;

    public void Init()
    {
        complementProduct = new SqlDouble(1);
    }

    public void Accumulate(SqlDouble Value)
    {
        // normalise value, divide by 100
        Value = System.Data.SqlTypes.SqlDouble.Divide(Value, new SqlDouble(100));
        // -x
        var negated = System.Data.SqlTypes.SqlDouble.Multiply(new SqlDouble(-1),Value);
        // 1-x
        var oneMinus = System.Data.SqlTypes.SqlDouble.Add(new SqlDouble(1), negated);
        complementProduct = System.Data.SqlTypes.SqlDouble.Multiply(complementProduct, oneMinus);
    }

    public void Merge (IndependentProject Group)
    {
        complementProduct = System.Data.SqlTypes.SqlDouble.Multiply(complementProduct,Group.complementProduct); 
    }

    public SqlDouble Terminate()
    {
        var negated = System.Data.SqlTypes.SqlDouble.Multiply(new SqlDouble(-1),complementProduct);
        var oneMinus = System.Data.SqlTypes.SqlDouble.Add(new SqlDouble(1), negated);
        return System.Data.SqlTypes.SqlDouble.Multiply(oneMinus, new SqlDouble(100));
    }

    // This is a place-holder member field
    public int _var1;
}
