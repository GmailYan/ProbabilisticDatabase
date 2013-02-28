using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ProbabilisticDatabase.Src.ControllerPackage.Query
{

    public class AttributeValue
    {
        private string attributeValue;

        public string AttributeValue1
        {
            get { return attributeValue; }
            set { attributeValue = value; }
        }

        public AttributeValue(string value)
        {
            attributeValue = value;
        }


    }
}
