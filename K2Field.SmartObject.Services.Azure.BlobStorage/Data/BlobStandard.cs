using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SourceCode.SmartObjects.Services.ServiceSDK.Objects;
using SourceCode.SmartObjects.Services.ServiceSDK.Types;

namespace K2Field.SmartObject.Services.Azure.BlobStorage.Data
{
    public static class BlobStandard
    {
        //public static List<Property> GetStandardInputProperties()
        //{
        //    List<Property> StandardInputProperties = new List<Property>();

        //    Property status = new Property();
        //    status.Name = "status";
        //    status.MetaData.DisplayName = "Status";
        //    status.SoType = SoType.Text;
        //    StandardInputProperties.Add(status);

        //    return StandardInputProperties;
        //}

        public static List<Property> GetStandardReturnProperties()
        {
            List<Property> StandardReturnProperties = new List<Property>();

            //StandardReturnProperties.AddRange(GetStandardInputProperties());

            Property responsestatus = new Property();
            responsestatus.Name = "responsestatus";
            responsestatus.MetaData.DisplayName = "Response Status";
            responsestatus.SoType = SoType.Text;
            StandardReturnProperties.Add(responsestatus);

            Property responsestatusdescription = new Property();
            responsestatusdescription.Name = "responsestatusdescription";
            responsestatusdescription.MetaData.DisplayName = "Response Status Description";
            responsestatusdescription.SoType = SoType.Memo;
            StandardReturnProperties.Add(responsestatusdescription);

            return StandardReturnProperties;

        }

    }
}
