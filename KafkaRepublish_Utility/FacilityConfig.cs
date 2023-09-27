using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaRepublish_Utility
{
    public class InductionFacility
    {
        public string FacilityName { get; set; }
        public string DataSource { get; set; }
    }

    public class FacilityConfig
    {
        public List<InductionFacility> InductionFacility { get; set; }
    }
}
