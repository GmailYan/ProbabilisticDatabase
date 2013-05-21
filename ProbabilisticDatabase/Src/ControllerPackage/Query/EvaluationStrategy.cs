using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProbabilisticDatabase.Src.ControllerPackage.Query
{
    public enum EvaluationStrategy
    {
        Default,
        Exact,
        Approximate,
        MonteCarlo
    }
}
