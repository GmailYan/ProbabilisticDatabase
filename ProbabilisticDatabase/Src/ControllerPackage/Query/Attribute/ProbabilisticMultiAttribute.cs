using System;
using System.Collections.Generic;

namespace ProbabilisticDatabase.Src.ControllerPackage.Query.Attribute
{
    public class ProbabilisticMultiAttribute : ProbabilisticAttribute
    {
        private List<List<string>> _multiAttrbutes; 
        private List<double> _pValues;

        public ProbabilisticMultiAttribute(List<List<string>> multiAttrbutes, List<double> pValues)
        {
            _multiAttrbutes = multiAttrbutes;
            _pValues = pValues;
        }

        public List<List<string>> MultiAttrbutes
        {
            get { return _multiAttrbutes; }
        }

        public List<double> PValues
        {
            get { return _pValues; }
        }
    }
}