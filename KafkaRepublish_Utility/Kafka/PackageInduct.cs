using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaRepublish_Utility.Kafka
{
    public  class PackageInduct
    {
        public string PrimaryBarcode { get; set; }
        public string LookupBarcodeMasked { get; set; }
        public string FastTrakSiteID { get; set; }
        public string ShipFromSiteID { get; set; }
        public string PkgInductedMode { get; set; }
        public string PkgInductedUser { get; set; }
        public string PackageScanXML { get; set; }
        public string SortID { get; set; }
        public string LabelData { get; set; }
        public DateTime InductionDateTimeUtc { get; set; }
        public int InductionDateTimeUtcOffset { get; set; }
    }

    public class PackageScan
    {
        [System.Xml.Serialization.XmlElement]
        public QualifierAttribute[] BarCode { get; set; }
    }

    [System.Xml.Serialization.XmlTypeAttribute]
    public class QualifierAttribute
    {
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string Qualifier { get; set; }

        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string Value { get; set; }
    }
}

