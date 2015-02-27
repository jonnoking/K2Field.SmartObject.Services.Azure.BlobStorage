using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using SourceCode.SmartObjects.Services.ServiceSDK.Types;
using SourceCode.SmartObjects.Services.ServiceSDK.Objects;
using SourceCode.SmartObjects.Services.ServiceSDK;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;


namespace K2Field.SmartObject.Services.Azure.BlobStorage.Data
{
    public class BlobBlob
    {
        private ServiceAssemblyBase serviceBroker = null;

        public BlobBlob(ServiceAssemblyBase serviceBroker)
        {
            // Set local serviceBroker variable.
            this.serviceBroker = serviceBroker;
        }
        /*
         * -- Blob --
         * Load Blob properties /
         * Delete blob from Container /
         * 
         * List container contents /
         * List container contents filter by metadata

         * Download to SOType.File /
         * Download to Filesystem /
         * Download to Base64 /
         * 
         * Upload from base64 /
         * Upload from Url /
         * Upload from Filesystem /
         * 
         * Set Blob Metadata /
         * Read Blob Metadata /
         * Read Blob Metadata Value /
         * 
         * Set Container Properties /
         * Read Container Properties value /
         * 
         * set directory as an input parameter for uploads
         * check forward slashes on directories
         * allow static creds, service account, etc on upload from url
         * Add length (size) property /
         * Add last modified property /

         */


        #region Describe

        public void Create()
        {
            List<Property> BlobProps = GetBlobProperties();

            ServiceObject BlobServiceObject = new ServiceObject();
            BlobServiceObject.Name = "azureblob";
            BlobServiceObject.MetaData.DisplayName = "Azure Blob";
            BlobServiceObject.MetaData.ServiceProperties.Add("objecttype", "blob");
            BlobServiceObject.Active = true;

            foreach (Property prop in BlobProps)
            {
                BlobServiceObject.Properties.Add(prop);
            }
            BlobServiceObject.Methods.Add(CreateLoadBlob(BlobProps));
            BlobServiceObject.Methods.Add(CreateDeleteBlob(BlobProps));

            BlobServiceObject.Methods.Add(CreateListBlobs(BlobProps));

            BlobServiceObject.Methods.Add(CreateLoadMetadataValue(BlobProps));
            BlobServiceObject.Methods.Add(CreateListMetadata(BlobProps));
            BlobServiceObject.Methods.Add(CreateSetMetadata(BlobProps));
            BlobServiceObject.Methods.Add(CreateUpdateBlobProperties(BlobProps));

            BlobServiceObject.Methods.Add(CreateUploadBlob(BlobProps));
            BlobServiceObject.Methods.Add(CreateUploadBlobFromPath(BlobProps));
            BlobServiceObject.Methods.Add(CreateUploadBlobFromUrl(BlobProps));
            BlobServiceObject.Methods.Add(CreateUploadBlobFromBase64(BlobProps));

            BlobServiceObject.Methods.Add(CreateLoadBlobToBase64(BlobProps));
            BlobServiceObject.Methods.Add(CreateLoadBlobToPath(BlobProps));
            BlobServiceObject.Methods.Add(CreateLoadBlobToSmartObject(BlobProps));
            

            serviceBroker.Service.ServiceObjects.Add(BlobServiceObject);

        }


        private List<Property> GetBlobProperties()
        {
            List<Property> ContainerProperties = new List<Property>();

            Property containername = new Property
            {
                Name = "containername",
                MetaData = new MetaData("Container Name", "Container Name"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(containername);

            Property name = new Property {
                Name = "blobname",
                MetaData = new MetaData("Blob Name", "Blob Name"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(name);

            Property uri = new Property
            {
                Name = "bloburi",
                MetaData = new MetaData("Blob Uri", "Blob Uri"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(uri);

            //Property props = new Property
            //{
            //    Name = "blobproperties",
            //    MetaData = new MetaData("Blob Properties", "Container Properties"),
            //    SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Memo,
            //    Type = "string"
            //};
            //ContainerProperties.Add(props);

            //Property propertykey = new Property
            //{
            //    Name = "blobpropertykey",
            //    MetaData = new MetaData("Blob Proptery Key", "Blob Property Key"),
            //    SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
            //    Type = "string"
            //};
            //ContainerProperties.Add(propertykey);

            //Property propertyvalue = new Property
            //{
            //    Name = "blobpropertyvalue",
            //    MetaData = new MetaData("Blob Property Value", "Blob Property Value"),
            //    SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
            //    Type = "string"
            //};
            //ContainerProperties.Add(propertyvalue);

            //Property metadata = new Property
            //{
            //    Name = "blobmetadata",
            //    MetaData = new MetaData("Blob Metadata", "Blob Metadata"),
            //    SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Memo,
            //    Type = "string"
            //};
            //ContainerProperties.Add(metadata);

            Property metadatakey = new Property
            {
                Name = "blobmetadatakey",
                MetaData = new MetaData("Blob Metadata Key", "Blob Metadata Key"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(metadatakey);

            Property metadatavalue = new Property
            {
                Name = "blobmetadatavalue",
                MetaData = new MetaData("Blob Metadata Value", "Blob Metadata Value"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(metadatavalue);

            Property blobtype = new Property
            {
                Name = "blobtype",
                MetaData = new MetaData("Blob Type", "Blob Type"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(blobtype);

            Property isnapshot = new Property
            {
                Name = "issnapshot",
                MetaData = new MetaData("Is Snapshot", "Is Snapshot"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.YesNo,
                Type = "bool"
            };
            ContainerProperties.Add(isnapshot);

            Property blobparent = new Property
            {
                Name = "blobparent",
                MetaData = new MetaData("Blob Parent", "Blob Parent"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(blobparent);

            Property snapshotqualifieduri = new Property
            {
                Name = "snapshotqualifieduri",
                MetaData = new MetaData("Snapshot Qualified Uri", "Snapshot Qualified Uri"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(snapshotqualifieduri);

            Property snapshottime = new Property
            {
                Name = "snapshottime",
                MetaData = new MetaData("Snapshot Time", "Snapshot Time"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(snapshottime);

            //Property createifdoesntexist = new Property
            //{
            //    Name = "createifdoesntexist",
            //    MetaData = new MetaData("Create If Doesnt Exist", "Create If Doesnt Exist"),
            //    SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.YesNo,
            //    Type = "boolean"
            //};
            //ContainerProperties.Add(createifdoesntexist);


            Property metadatacount = new Property
            {
                Name = "metadatacount",
                MetaData = new MetaData("Metadata Count", "Metadata Count"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Number,
                Type = "int"
            };
            ContainerProperties.Add(metadatacount);

            Property base64 = new Property
            {
                Name = "filebase64",
                MetaData = new MetaData("File Base64", "File Base64"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Memo,
                Type = "string"
            };
            ContainerProperties.Add(base64);

            Property directoryname = new Property
            {
                Name = "directory",
                MetaData = new MetaData("Directory", "Directory"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(directoryname);

            Property filepath = new Property
            {
                Name = "filepath",
                MetaData = new MetaData("File Path", "File Path"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(filepath);

            Property blobfile = new Property
            {
                Name = "blobfile",
                MetaData = new MetaData("Blob File", "Blob File"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.File,
                Type = "object"
            };
            ContainerProperties.Add(blobfile);

            Property flattenlist = new Property
            {
                Name = "flattenlist",
                MetaData = new MetaData("flatten List", "flatten List"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.YesNo,
                Type = "bool"
            };
            ContainerProperties.Add(flattenlist);

            Property fileuri = new Property
            {
                Name = "fileurl",
                MetaData = new MetaData("File Url", "File Url"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(fileuri);

            Property deletetype = new Property
            {
                Name = "deletetype",
                MetaData = new MetaData("Delete Type", "Delete Type"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(deletetype);

            Property blobsize = new Property
            {
                Name = "blobsize",
                MetaData = new MetaData("Blob Size", "Blob Size"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Number,
                Type = "int"
            };
            ContainerProperties.Add(blobsize);

            Property lastmodified = new Property
            {
                Name = "lastmodified",
                MetaData = new MetaData("Last Modified", "Last Modified"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.DateTime,
                Type = "DateTime"
            };
            ContainerProperties.Add(lastmodified);

            Property leasestate = new Property
            {
                Name = "leasestate",
                MetaData = new MetaData("Lease State", "Lease State"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(leasestate);

            Property leasestatus = new Property
            {
                Name = "leasestatus",
                MetaData = new MetaData("Lease Status", "Lease Status"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(leasestatus);

            Property contentencoding = new Property
            {
                Name = "contentencoding",
                MetaData = new MetaData("Content Encoding", "Content Encoding"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(contentencoding);

            Property contenttype = new Property
            {
                Name = "contenttype",
                MetaData = new MetaData("Content Type", "Content Type"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(contenttype);
            ContainerProperties.AddRange(BlobStandard.GetStandardReturnProperties());

            return ContainerProperties;
        }

        private Method CreateLoadBlob(List<Property> BlobProps)
        {
            Method LoadBlob = new Method();
            LoadBlob.Name = "loadblob";
            LoadBlob.MetaData.DisplayName = "Load Blob";
            LoadBlob.Type = MethodType.Read;
            LoadBlob.InputProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            LoadBlob.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            LoadBlob.InputProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            LoadBlob.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());

            foreach (Property prop in BlobProps)
            {
                LoadBlob.ReturnProperties.Add(prop);
            }
            return LoadBlob;
        }

        private Method CreateSetMetadata(List<Property> BlobProps)
        {
            Method CreateSetMetadata = new Method();
            CreateSetMetadata.Name = "setblobmetadata";
            CreateSetMetadata.MetaData.DisplayName = "Set Blob Metadata";
            CreateSetMetadata.Type = MethodType.Update;
            CreateSetMetadata.InputProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateSetMetadata.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateSetMetadata.InputProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            CreateSetMetadata.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            CreateSetMetadata.InputProperties.Add(BlobProps.Where(p => p.Name == "blobmetadatakey").First());
            CreateSetMetadata.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "blobmetadatakey").First());
            CreateSetMetadata.InputProperties.Add(BlobProps.Where(p => p.Name == "blobmetadatavalue").First());
            CreateSetMetadata.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "blobmetadatavalue").First());

            // need to trim return properties
            foreach (Property prop in BlobProps)
            {
                CreateSetMetadata.ReturnProperties.Add(prop);
            }
            return CreateSetMetadata;
        }

        private Method CreateLoadMetadataValue(List<Property> BlobProps)
        {
            Method CreateLoadMetadataValue = new Method();
            CreateLoadMetadataValue.Name = "loadblobmetadatavalue";
            CreateLoadMetadataValue.MetaData.DisplayName = "Load Blob Metadata Value";
            CreateLoadMetadataValue.Type = MethodType.Read;
            CreateLoadMetadataValue.InputProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateLoadMetadataValue.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateLoadMetadataValue.InputProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            CreateLoadMetadataValue.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            CreateLoadMetadataValue.InputProperties.Add(BlobProps.Where(p => p.Name == "blobmetadatakey").First());
            CreateLoadMetadataValue.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "blobmetadatakey").First());

            CreateLoadMetadataValue.ReturnProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateLoadMetadataValue.ReturnProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            CreateLoadMetadataValue.ReturnProperties.Add(BlobProps.Where(p => p.Name == "blobmetadatakey").First());
            CreateLoadMetadataValue.ReturnProperties.Add(BlobProps.Where(p => p.Name == "blobmetadatavalue").First());
            CreateLoadMetadataValue.ReturnProperties.Add(BlobProps.Where(p => p.Name == "responsestatus").First());
            CreateLoadMetadataValue.ReturnProperties.Add(BlobProps.Where(p => p.Name == "responsestatusdescription").First());
            
            // need to trim return properties
            //foreach (Property prop in ContainerProps)
            //{
            //    CreateLoadMetadataValue.ReturnProperties.Add(prop);
            //}
            return CreateLoadMetadataValue;
        }

        private Method CreateListMetadata(List<Property> BlobProps)
        {
            Method CreateListMetadata = new Method();
            CreateListMetadata.Name = "listblobmetadata";
            CreateListMetadata.MetaData.DisplayName = "List Blob Metadata";
            CreateListMetadata.Type = MethodType.List;
            CreateListMetadata.InputProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateListMetadata.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateListMetadata.InputProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            CreateListMetadata.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());

            CreateListMetadata.ReturnProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateListMetadata.ReturnProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            CreateListMetadata.ReturnProperties.Add(BlobProps.Where(p => p.Name == "blobmetadatakey").First());
            CreateListMetadata.ReturnProperties.Add(BlobProps.Where(p => p.Name == "blobmetadatavalue").First());
            CreateListMetadata.ReturnProperties.Add(BlobProps.Where(p => p.Name == "responsestatus").First());
            CreateListMetadata.ReturnProperties.Add(BlobProps.Where(p => p.Name == "responsestatusdescription").First());

            // need to trim return properties
            //foreach (Property prop in ContainerProps)
            //{
            //    CreateLoadMetadataValue.ReturnProperties.Add(prop);
            //}
            return CreateListMetadata;
        }

        private Method CreateDeleteBlob(List<Property> BlobProps)
        {
            Method CreateDeleteBlob = new Method();
            CreateDeleteBlob.Name = "deleteblob";
            CreateDeleteBlob.MetaData.DisplayName = "Delete Blob";
            CreateDeleteBlob.Type = MethodType.Delete;
            CreateDeleteBlob.InputProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateDeleteBlob.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateDeleteBlob.InputProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            CreateDeleteBlob.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            CreateDeleteBlob.InputProperties.Add(BlobProps.Where(p => p.Name == "deletetype").First());
            CreateDeleteBlob.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "deletetype").First());

            CreateDeleteBlob.ReturnProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateDeleteBlob.ReturnProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            CreateDeleteBlob.ReturnProperties.Add(BlobProps.Where(p => p.Name == "deletetype").First());
            CreateDeleteBlob.ReturnProperties.Add(BlobProps.Where(p => p.Name == "responsestatus").First());
            CreateDeleteBlob.ReturnProperties.Add(BlobProps.Where(p => p.Name == "responsestatusdescription").First());

            // need to trim return properties
            //foreach (Property prop in ContainerProps)
            //{
            //    CreateLoadMetadataValue.ReturnProperties.Add(prop);
            //}
            return CreateDeleteBlob;
        }

        private Method CreateUploadBlob(List<Property> BlobProps)
        {
            Method CreateUploadBlob = new Method();
            CreateUploadBlob.Name = "uploadblob";
            CreateUploadBlob.MetaData.DisplayName = "Upload Blob";
            CreateUploadBlob.Type = MethodType.Create;
            CreateUploadBlob.InputProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateUploadBlob.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateUploadBlob.InputProperties.Add(BlobProps.Where(p => p.Name == "blobfile").First());
            CreateUploadBlob.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "blobfile").First());
            CreateUploadBlob.InputProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            //CreateUploadBlob.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            CreateUploadBlob.InputProperties.Add(BlobProps.Where(p => p.Name == "directory").First());

            foreach (Property prop in BlobProps)
            {
                CreateUploadBlob.ReturnProperties.Add(prop);
            }
            return CreateUploadBlob;
        }


        private Method CreateUploadBlobFromBase64(List<Property> BlobProps)
        {
            Method CreateUploadBlobFromBase64 = new Method();
            CreateUploadBlobFromBase64.Name = "uploadblobfrombase64";
            CreateUploadBlobFromBase64.MetaData.DisplayName = "Upload Blob From Base64";
            CreateUploadBlobFromBase64.Type = MethodType.Create;
            CreateUploadBlobFromBase64.InputProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateUploadBlobFromBase64.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateUploadBlobFromBase64.InputProperties.Add(BlobProps.Where(p => p.Name == "filebase64").First());
            CreateUploadBlobFromBase64.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "filebase64").First());
            CreateUploadBlobFromBase64.InputProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            CreateUploadBlobFromBase64.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            CreateUploadBlobFromBase64.InputProperties.Add(BlobProps.Where(p => p.Name == "directory").First());

            foreach (Property prop in BlobProps)
            {
                CreateUploadBlobFromBase64.ReturnProperties.Add(prop);
            }
            return CreateUploadBlobFromBase64;
        }

        private Method CreateUploadBlobFromPath(List<Property> BlobProps)
        {
            Method CreateUploadBlobFromPath = new Method();
            CreateUploadBlobFromPath.Name = "uploadblobfromfilesystem";
            CreateUploadBlobFromPath.MetaData.DisplayName = "Upload Blob From File System";
            CreateUploadBlobFromPath.Type = MethodType.Create;
            CreateUploadBlobFromPath.InputProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateUploadBlobFromPath.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateUploadBlobFromPath.InputProperties.Add(BlobProps.Where(p => p.Name == "filepath").First());
            CreateUploadBlobFromPath.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "filepath").First());
            CreateUploadBlobFromPath.InputProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            //CreateUploadBlobFromPath.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            CreateUploadBlobFromPath.InputProperties.Add(BlobProps.Where(p => p.Name == "directory").First());

            foreach (Property prop in BlobProps)
            {
                CreateUploadBlobFromPath.ReturnProperties.Add(prop);
            }
            return CreateUploadBlobFromPath;
        }

        private Method CreateUploadBlobFromUrl(List<Property> BlobProps)
        {
            Method CreateUploadBlobFromUrl = new Method();
            CreateUploadBlobFromUrl.Name = "uploadblobfromurl";
            CreateUploadBlobFromUrl.MetaData.DisplayName = "Upload Blob From Url";
            CreateUploadBlobFromUrl.Type = MethodType.Create;
            CreateUploadBlobFromUrl.InputProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateUploadBlobFromUrl.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateUploadBlobFromUrl.InputProperties.Add(BlobProps.Where(p => p.Name == "fileurl").First());
            CreateUploadBlobFromUrl.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "fileurl").First());
            CreateUploadBlobFromUrl.InputProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            //CreateUploadBlobFromPath.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            CreateUploadBlobFromUrl.InputProperties.Add(BlobProps.Where(p => p.Name == "directory").First());

            foreach (Property prop in BlobProps)
            {
                CreateUploadBlobFromUrl.ReturnProperties.Add(prop);
            }
            return CreateUploadBlobFromUrl;
        }

        private Method CreateListBlobs(List<Property> BlobProps)
        {
            Method CreateListBlobs = new Method();
            CreateListBlobs.Name = "listblobs";
            CreateListBlobs.MetaData.DisplayName = "List Blobs";
            CreateListBlobs.Type = MethodType.List;
            CreateListBlobs.InputProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateListBlobs.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            CreateListBlobs.InputProperties.Add(BlobProps.Where(p => p.Name == "directory").First());
            //CreateListBlobs.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "directory").First());
            CreateListBlobs.InputProperties.Add(BlobProps.Where(p => p.Name == "flattenlist").First());
            //CreateListBlobs.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "flattenlist").First());

            // need to trim return properties
            foreach (Property prop in BlobProps)
            {
                CreateListBlobs.ReturnProperties.Add(prop);
            }
            return CreateListBlobs;
        }

        private Method CreateLoadBlobToBase64(List<Property> BlobProps)
        {
            Method LoadBlobToBase64 = new Method();
            LoadBlobToBase64.Name = "downloadblobtobase64";
            LoadBlobToBase64.MetaData.DisplayName = "Download Blob To Base64";
            LoadBlobToBase64.Type = MethodType.Read;
            LoadBlobToBase64.InputProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            LoadBlobToBase64.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            LoadBlobToBase64.InputProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            LoadBlobToBase64.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());

            foreach (Property prop in BlobProps)
            {
                LoadBlobToBase64.ReturnProperties.Add(prop);
            }
            return LoadBlobToBase64;
        }

        private Method CreateLoadBlobToPath(List<Property> BlobProps)
        {
            Method LoadBlobToPath = new Method();
            LoadBlobToPath.Name = "downloadblobtofilesystem";
            LoadBlobToPath.MetaData.DisplayName = "Download Blob To File System";
            LoadBlobToPath.Type = MethodType.Read;
            LoadBlobToPath.InputProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            LoadBlobToPath.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            LoadBlobToPath.InputProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            LoadBlobToPath.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            LoadBlobToPath.InputProperties.Add(BlobProps.Where(p => p.Name == "filepath").First());
            LoadBlobToPath.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "filepath").First());

            foreach (Property prop in BlobProps)
            {
                LoadBlobToPath.ReturnProperties.Add(prop);
            }
            return LoadBlobToPath;
        }

        private Method CreateLoadBlobToSmartObject(List<Property> BlobProps)
        {
            Method LoadBlobToPath = new Method();
            LoadBlobToPath.Name = "downloadblob";
            LoadBlobToPath.MetaData.DisplayName = "Download Blob";
            LoadBlobToPath.Type = MethodType.Read;
            LoadBlobToPath.InputProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            LoadBlobToPath.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "containername").First());
            LoadBlobToPath.InputProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());
            LoadBlobToPath.Validation.RequiredProperties.Add(BlobProps.Where(p => p.Name == "blobname").First());

            foreach (Property prop in BlobProps)
            {
                LoadBlobToPath.ReturnProperties.Add(prop);
            }
            return LoadBlobToPath;
        }

        private Method CreateUpdateBlobProperties(List<Property> ContainerProps)
        {
            Method CreateUploadBlobFromPath = new Method();
            CreateUploadBlobFromPath.Name = "setblobproperties";
            CreateUploadBlobFromPath.MetaData.DisplayName = "Set Blob Properties";
            CreateUploadBlobFromPath.Type = MethodType.Update;
            CreateUploadBlobFromPath.InputProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            CreateUploadBlobFromPath.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            CreateUploadBlobFromPath.InputProperties.Add(ContainerProps.Where(p => p.Name == "blobname").First());
            CreateUploadBlobFromPath.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "blobname").First());
            CreateUploadBlobFromPath.InputProperties.Add(ContainerProps.Where(p => p.Name == "contenttype").First());
            CreateUploadBlobFromPath.InputProperties.Add(ContainerProps.Where(p => p.Name == "contentencoding").First());

            foreach (Property prop in ContainerProps)
            {
                CreateUploadBlobFromPath.ReturnProperties.Add(prop);
            }
            return CreateUploadBlobFromPath;
        }

        #endregion Describe




        #region Execute


        public void ExecuteLoadBlob(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;
            CloudBlockBlob blob;

            try
            {
                account = blobUtils.GetStorageAccount();
                client = blobUtils.GetBlobClient(account);

                container = blobUtils.GetBlobContainer(client, inputs.Where(p => p.Name.Equals("containername", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), true,
                    false, false);

                if (container == null)
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container not found";
                }
                else
                {
                    blob = blobUtils.GetBlobProperties(container, inputs.Where(p => p.Name.Equals("blobname", StringComparison.OrdinalIgnoreCase)).First().Value.ToString());

                    if (blob == null)
                    {
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                        returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Blob not found";
                    }
                    else
                    {

                        foreach (Property prop in returns)
                        {
                            switch (prop.Name.ToLower())
                            {
                                case "containername":
                                    prop.Value = container.Name;
                                    break;
                                case "blobname":
                                    prop.Value = blob.Name;
                                    break;
                                case "bloburi":
                                    prop.Value = blob.Uri.ToString();
                                    break;
                                case "blobtype":
                                    prop.Value = blob.BlobType.ToString();
                                    break;
                                case "blobparent":
                                    if (blob.Parent != null && blob.Parent.Container != null)
                                    {
                                        prop.Value = blob.Parent.Container.Name;
                                    }
                                    break;
                                case "metadatacount":
                                    prop.Value = blob.Metadata.Count;
                                    break;
                                case "lastmodified":
                                    if (blob.Properties.LastModified.HasValue)
                                    {
                                        prop.Value = blob.Properties.LastModified.Value.DateTime;
                                    }
                                    break;
                                case "blobsize":
                                    prop.Value = blob.Properties.Length;
                                    break;
                                case "leasestate":
                                    prop.Value = blob.Properties.LeaseState;
                                    break;
                                case "leasestatus":
                                    prop.Value = blob.Properties.LeaseStatus;
                                    break;
                                case "contentencoding":
                                    prop.Value = blob.Properties.ContentEncoding;
                                    break;
                                case "contenttype":
                                    prop.Value = blob.Properties.ContentType;
                                    break;
                            }
                        }
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                        //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container deleted";
                    }
                }
            }
            catch (StorageException ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                account = null;
                client = null;
                container = null;
                blob = null;
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteSetBlobMetadata(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;
            CloudBlockBlob blob;

            try
            {
                account = blobUtils.GetStorageAccount();
                client = blobUtils.GetBlobClient(account);

                container = blobUtils.GetBlobContainer(client, inputs.Where(p => p.Name.Equals("containername", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), true,
                    false, false);

                if (container == null)
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container not found";
                }
                else
                {
                    blob = blobUtils.GetBlobProperties(container, inputs.Where(p => p.Name.Equals("blobname", StringComparison.OrdinalIgnoreCase)).First().Value.ToString());

                    if (blob == null)
                    {
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                        returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Blob not found";
                    }
                    else
                    {

                        string mdkey = string.Empty;
                        Property prp = inputs.Where(p => p.Name.Equals("blobmetadatakey", StringComparison.OrdinalIgnoreCase)).First();
                        if (prp != null && prp.Value != null)
                        {
                            mdkey = prp.Value.ToString();
                        }

                        string mdvalue = string.Empty;
                        Property prp1 = inputs.Where(p => p.Name.Equals("blobmetadatavalue", StringComparison.OrdinalIgnoreCase)).First();
                        if (prp1 != null && prp1.Value != null)
                        {
                            mdvalue = prp1.Value.ToString();
                        }

                        blob = blobUtils.SetBlobMetadata(blob, mdkey, mdvalue);

                        foreach (Property prop in returns)
                        {
                            switch (prop.Name.ToLower())
                            {
                                case "containername":
                                    prop.Value = container.Name;
                                    break;
                                case "blobname":
                                    prop.Value = blob.Name;
                                    break;
                                case "bloburi":
                                    prop.Value = blob.Uri.ToString();
                                    break;
                                case "blobtype":
                                    prop.Value = blob.BlobType.ToString();
                                    break;
                                case "blobparent":
                                    if (blob.Parent != null && blob.Parent.Container != null)
                                    {
                                        prop.Value = blob.Parent.Container.Name;
                                    }
                                    break;
                                case "metadatacount":
                                    prop.Value = blob.Metadata.Count;
                                    break;
                                case "blobmetadatakey":
                                    prop.Value = mdkey;
                                    break;
                                case "blobmetadatavalue":
                                    prop.Value = mdvalue;
                                    break;
                                case "lastmodified":
                                    if (blob.Properties.LastModified.HasValue)
                                    {
                                        prop.Value = blob.Properties.LastModified.Value.DateTime;
                                    }
                                    break;
                                case "blobsize":
                                    prop.Value = blob.Properties.Length;
                                    break;
                                case "leasestate":
                                    prop.Value = blob.Properties.LeaseState;
                                    break;
                                case "leasestatus":
                                    prop.Value = blob.Properties.LeaseStatus;
                                    break;
                                case "contentencoding":
                                    prop.Value = blob.Properties.ContentEncoding;
                                    break;
                                case "contenttype":
                                    prop.Value = blob.Properties.ContentType;
                                    break;

                            }
                        }
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                        //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container deleted";
                    }
                }
            }
            catch (StorageException ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                account = null;
                client = null;
                container = null;
                blob = null;
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteLoadBlobMetadataValue(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;
            CloudBlockBlob blob;

            try
            {
                account = blobUtils.GetStorageAccount();
                client = blobUtils.GetBlobClient(account);

                container = blobUtils.GetBlobContainer(client, inputs.Where(p => p.Name.Equals("containername", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), true,
                    false, false);

                if (container == null)
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container not found";
                }
                else
                {
                    blob = blobUtils.GetBlobProperties(container, inputs.Where(p => p.Name.Equals("blobname", StringComparison.OrdinalIgnoreCase)).First().Value.ToString());

                    if (blob == null)
                    {
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                        returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Blob not found";
                    }
                    else
                    {

                        string mdkey = string.Empty;
                        Property prp = inputs.Where(p => p.Name.Equals("blobmetadatakey", StringComparison.OrdinalIgnoreCase)).First();
                        if (prp != null && prp.Value != null)
                        {
                            mdkey = prp.Value.ToString();
                        }

                        string mdvalue = string.Empty;

                        Dictionary<string, string> md = blob.Metadata as Dictionary<string, string>;

                        mdvalue = md[mdkey];

                        foreach (Property prop in returns)
                        {
                            switch (prop.Name.ToLower())
                            {
                                case "containername":
                                    prop.Value = container.Name;
                                    break;
                                case "blobname":
                                    prop.Value = blob.Name;
                                    break;
                                case "bloburi":
                                    prop.Value = blob.Uri.ToString();
                                    break;
                                case "blobtype":
                                    prop.Value = blob.BlobType.ToString();
                                    break;
                                case "blobparent":
                                    if (blob.Parent != null && blob.Parent.Container != null)
                                    {
                                        prop.Value = blob.Parent.Container.Name;
                                    }
                                    break;
                                case "metadatacount":
                                    prop.Value = blob.Metadata.Count;
                                    break;
                                case "blobmetadatakey":
                                    prop.Value = mdkey;
                                    break;
                                case "blobmetadatavalue":
                                    prop.Value = mdvalue;
                                    break;
                                case "lastmodified":
                                    if (blob.Properties.LastModified.HasValue)
                                    {
                                        prop.Value = blob.Properties.LastModified.Value.DateTime;
                                    }
                                    break;
                                case "blobsize":
                                    prop.Value = blob.Properties.Length;
                                    break;
                                case "leasestate":
                                    prop.Value = blob.Properties.LeaseState;
                                    break;
                                case "leasestatus":
                                    prop.Value = blob.Properties.LeaseStatus;
                                    break;
                                case "contentencoding":
                                    prop.Value = blob.Properties.ContentEncoding;
                                    break;
                                case "contenttype":
                                    prop.Value = blob.Properties.ContentType;
                                    break;
                            }
                        }
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                        //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container deleted";
                    }
                }
            }
            catch (StorageException ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                account = null;
                client = null;
                container = null;
                blob = null;
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteListBlobMetadata(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;
            CloudBlockBlob blob;

            System.Data.DataRow dr;

            try
            {
                account = blobUtils.GetStorageAccount();
                client = blobUtils.GetBlobClient(account);

                container = blobUtils.GetBlobContainer(client, inputs.Where(p => p.Name.Equals("containername", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), true,
                    false, false);

                if (container == null)
                {
                    dr = serviceBroker.ServicePackage.ResultTable.NewRow();
                    dr["responsestatus"] = ResponseStatus.Error;
                    dr["responsestatusdescription"] = "Container not found";
                    serviceBroker.ServicePackage.ResultTable.Rows.Add(dr);

                }
                else
                {
                    blob = blobUtils.GetBlobProperties(container, inputs.Where(p => p.Name.Equals("blobname", StringComparison.OrdinalIgnoreCase)).First().Value.ToString());

                    if (blob == null)
                    {
                        dr = serviceBroker.ServicePackage.ResultTable.NewRow();
                        dr["responsestatus"] = ResponseStatus.Error;
                        dr["responsestatusdescription"] = "Blob not found";
                        serviceBroker.ServicePackage.ResultTable.Rows.Add(dr);
                    }
                    else
                    {

                        Dictionary<string, string> md = blob.Metadata as Dictionary<string, string>;

                        foreach (KeyValuePair<string, string> data in md)
                        {
                            dr = serviceBroker.ServicePackage.ResultTable.NewRow();

                            foreach (Property prop in returns)
                            {
                                switch (prop.Name.ToLower())
                                {
                                    case "containername":
                                        dr[prop.Name] = container.Name;
                                        break;
                                    case "blobname":
                                        dr[prop.Name] = blob.Name;
                                        break;
                                    case "blobmetadatakey":
                                        dr[prop.Name] = data.Key;
                                        break;
                                    case "blobmetadatavalue":
                                        dr[prop.Name] = data.Value;
                                        break;
                                }
                            }
                            returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                            //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Metadata loaded";
                            serviceBroker.ServicePackage.ResultTable.Rows.Add(dr);
                        }
                    }
                }
            }
            catch (StorageException ex)
            {
                dr = serviceBroker.ServicePackage.ResultTable.NewRow();
                dr["responsestatus"] = ResponseStatus.Error;
                dr["responsestatusdescription"] = ex.Message;
                serviceBroker.ServicePackage.ResultTable.Rows.Add(dr);
            }
            catch (Exception ex)
            {
                dr = serviceBroker.ServicePackage.ResultTable.NewRow();
                dr["responsestatus"] = ResponseStatus.Error;
                dr["responsestatusdescription"] = ex.Message;
                serviceBroker.ServicePackage.ResultTable.Rows.Add(dr);
            }
            finally
            {
                account = null;
                client = null;
                container = null;
                blob = null;
            }
            //serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteDeleteBlob(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;
            CloudBlockBlob blob;

            try
            {
                account = blobUtils.GetStorageAccount();
                client = blobUtils.GetBlobClient(account);

                container = blobUtils.GetBlobContainer(client, inputs.Where(p => p.Name.Equals("containername", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), true,
                    false, false);

                if (container == null)
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container not found";
                }
                else
                {
                    blob = blobUtils.GetBlobProperties(container, inputs.Where(p => p.Name.Equals("blobname", StringComparison.OrdinalIgnoreCase)).First().Value.ToString());

                    if (blob == null)
                    {
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                        returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Blob not found";
                    }
                    else
                    {
                        blobUtils.DeleteBlob(blob, inputs.Where(p => p.Name.Equals("deletetype", StringComparison.OrdinalIgnoreCase)).First().Value.ToString());

                        foreach (Property prop in returns)
                        {
                            switch (prop.Name.ToLower())
                            {
                                case "containername":
                                    prop.Value = container.Name;
                                    break;
                                case "blobname":
                                    prop.Value = blob.Name;
                                    break;
                                case "deletetype":
                                    prop.Value = inputs.Where(p => p.Name.Equals("deletetype", StringComparison.OrdinalIgnoreCase)).First().Value.ToString();
                                    break;
                            }
                        }
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                        //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container deleted";
                    }
                }
            }
            catch (StorageException ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                account = null;
                client = null;
                container = null;
                blob = null;
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteUploadBlob(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;
            CloudBlockBlob blob;

            try
            {
                account = blobUtils.GetStorageAccount();
                client = blobUtils.GetBlobClient(account);

                container = blobUtils.GetBlobContainer(client, inputs.Where(p => p.Name.Equals("containername", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), true,
                    false, false);

                if (container == null)
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container not found";
                }
                else
                {
                    FileProperty upload = inputs.Where(p => p.Name.Equals("blobfile", StringComparison.OrdinalIgnoreCase)).First() as FileProperty;

                    string blobname = string.Empty;
                    Property prp = inputs.Where(p => p.Name.Equals("blobname", StringComparison.OrdinalIgnoreCase)).First();
                    if (prp != null && prp.Value != null)
                    {
                        blobname = prp.Value.ToString();
                    }
                    
                    if (string.IsNullOrWhiteSpace(blobname)) 
                    {
                        if (string.IsNullOrWhiteSpace(upload.FileName))
                        {
                            blobname = Guid.NewGuid().ToString();
                        }
                        else
                        {
                            blobname = upload.FileName;
                        }
                    }

                    string path = string.Empty;
                    Property pathProp = inputs.Where(p => p.Name.Equals("directory", StringComparison.OrdinalIgnoreCase)).First();
                    if (pathProp != null && pathProp.Value != null)
                    {
                        path = pathProp.Value.ToString();
                    }

                    path = blobUtils.GetPath(path);

                    blob = blobUtils.UploadBlobFromBase64(container, upload.Content, path, blobname);

                    if (blob == null)
                    {
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                        returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Upload failed. Blob not found";
                    }
                    else
                    {
                        foreach (Property prop in returns)
                        {
                            switch (prop.Name.ToLower())
                            {
                                case "containername":
                                    prop.Value = container.Name;
                                    break;
                                case "directory":
                                    prop.Value = path;
                                    break;
                                case "blobname":
                                    prop.Value = blob.Name;
                                    break;
                                case "bloburi":
                                    prop.Value = blob.Uri.ToString();
                                    break;
                                case "blobtype":
                                    prop.Value = blob.BlobType.ToString();
                                    break;
                                case "blobparent":
                                    if (blob.Parent != null && blob.Parent.Container != null)
                                    {
                                        prop.Value = blob.Parent.Container.Name;
                                    }
                                    break;
                                case "metadatacount":
                                    prop.Value = blob.Metadata.Count;
                                    break;
                                case "lastmodified":
                                    if (blob.Properties.LastModified.HasValue)
                                    {
                                        prop.Value = blob.Properties.LastModified.Value.DateTime;
                                    }
                                    break;
                                case "blobsize":
                                    prop.Value = blob.Properties.Length;
                                    break;
                                case "leasestate":
                                    prop.Value = blob.Properties.LeaseState;
                                    break;
                                case "leasestatus":
                                    prop.Value = blob.Properties.LeaseStatus;
                                    break;
                                case "contentencoding":
                                    prop.Value = blob.Properties.ContentEncoding;
                                    break;
                                case "contenttype":
                                    prop.Value = blob.Properties.ContentType;
                                    break;
                            }
                        }
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                        //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container deleted";
                    }
                }
            }
            catch (StorageException ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                account = null;
                client = null;
                container = null;
                blob = null;
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteUploadBlobFromBase64(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;
            CloudBlockBlob blob;

            try
            {
                account = blobUtils.GetStorageAccount();
                client = blobUtils.GetBlobClient(account);

                container = blobUtils.GetBlobContainer(client, inputs.Where(p => p.Name.Equals("containername", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), true,
                    false, false);

                if (container == null)
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container not found";
                }
                else
                {
                    string path = string.Empty;
                    Property pathProp = inputs.Where(p => p.Name.Equals("directory", StringComparison.OrdinalIgnoreCase)).First();
                    if (pathProp != null && pathProp.Value != null)
                    {
                        path = pathProp.Value.ToString();
                    }

                    path = blobUtils.GetPath(path);

                    blob = blobUtils.UploadBlobFromBase64(container, inputs.Where(p => p.Name.Equals("filebase64", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), path, inputs.Where(p => p.Name.Equals("blobname", StringComparison.OrdinalIgnoreCase)).First().Value.ToString());

                    if (blob == null)
                    {
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                        returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Upload failed. Blob not found";
                    }
                    else
                    {
                        foreach (Property prop in returns)
                        {
                            switch (prop.Name.ToLower())
                            {
                                case "containername":
                                    prop.Value = container.Name;
                                    break;
                                case "directory":
                                    prop.Value = path;
                                    break;
                                case "blobname":
                                    prop.Value = blob.Name;
                                    break;
                                case "bloburi":
                                    prop.Value = blob.Uri.ToString();
                                    break;
                                case "blobtype":
                                    prop.Value = blob.BlobType.ToString();
                                    break;
                                case "blobparent":
                                    if (blob.Parent != null && blob.Parent.Container != null)
                                    {
                                        prop.Value = blob.Parent.Container.Name;
                                    }
                                    break;
                                case "metadatacount":
                                    prop.Value = blob.Metadata.Count;
                                    break;
                                case "lastmodified":
                                    if (blob.Properties.LastModified.HasValue)
                                    {
                                        prop.Value = blob.Properties.LastModified.Value.DateTime;
                                    }
                                    break;
                                case "blobsize":
                                    prop.Value = blob.Properties.Length;
                                    break;
                                case "leasestate":
                                    prop.Value = blob.Properties.LeaseState;
                                    break;
                                case "leasestatus":
                                    prop.Value = blob.Properties.LeaseStatus;
                                    break;
                                case "contentencoding":
                                    prop.Value = blob.Properties.ContentEncoding;
                                    break;
                                case "contenttype":
                                    prop.Value = blob.Properties.ContentType;
                                    break;
                            }
                        }
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                        //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container deleted";
                    }
                }
            }
            catch (StorageException ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                account = null;
                client = null;
                container = null;
                blob = null;
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteUploadBlobFromFilePath(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;
            CloudBlockBlob blob;

            try
            {
                account = blobUtils.GetStorageAccount();
                client = blobUtils.GetBlobClient(account);

                container = blobUtils.GetBlobContainer(client, inputs.Where(p => p.Name.Equals("containername", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), true,
                    false, false);

                if (container == null)
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container not found";
                }
                else
                {
                    string blobname = string.Empty;
                    Property prp = inputs.Where(p => p.Name.Equals("blobname", StringComparison.OrdinalIgnoreCase)).First();
                    if (prp != null && prp.Value != null)
                    {
                        blobname = prp.Value.ToString();
                    }

                    string path = string.Empty;
                    Property pathProp = inputs.Where(p => p.Name.Equals("directory", StringComparison.OrdinalIgnoreCase)).First();
                    if (pathProp != null && pathProp.Value != null)
                    {
                        path = pathProp.Value.ToString();
                    }

                    path = blobUtils.GetPath(path);

                    blob = blobUtils.UploadBlobFromFilesystem(container, inputs.Where(p => p.Name.Equals("filepath", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), path, blobname);

                    if (blob == null)
                    {
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                        returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Upload failed. Blob not found";
                    }
                    else
                    {
                        foreach (Property prop in returns)
                        {
                            switch (prop.Name.ToLower())
                            {
                                case "containername":
                                    prop.Value = container.Name;
                                    break;
                                case "directory":
                                    prop.Value = path;
                                    break;
                                case "blobname":
                                    prop.Value = blob.Name;
                                    break;
                                case "bloburi":
                                    prop.Value = blob.Uri.ToString();
                                    break;
                                case "blobtype":
                                    prop.Value = blob.BlobType.ToString();
                                    break;
                                case "blobparent":
                                    if (blob.Parent != null && blob.Parent.Container != null)
                                    {
                                        prop.Value = blob.Parent.Container.Name;
                                    }
                                    break;
                                case "metadatacount":
                                    prop.Value = blob.Metadata.Count;
                                    break;
                                case "lastmodified":
                                    if (blob.Properties.LastModified.HasValue)
                                    {
                                        prop.Value = blob.Properties.LastModified.Value.DateTime;
                                    }
                                    break;
                                case "blobsize":
                                    prop.Value = blob.Properties.Length;
                                    break;
                                case "leasestate":
                                    prop.Value = blob.Properties.LeaseState;
                                    break;
                                case "leasestatus":
                                    prop.Value = blob.Properties.LeaseStatus;
                                    break;
                                case "contentencoding":
                                    prop.Value = blob.Properties.ContentEncoding;
                                    break;
                                case "contenttype":
                                    prop.Value = blob.Properties.ContentType;
                                    break;
                            }
                        }
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                        //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container deleted";
                    }
                }
            }
            catch (StorageException ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                account = null;
                client = null;
                container = null;
                blob = null;
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteUploadBlobFromUrl(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;
            CloudBlockBlob blob;

            try
            {
                account = blobUtils.GetStorageAccount();
                client = blobUtils.GetBlobClient(account);

                container = blobUtils.GetBlobContainer(client, inputs.Where(p => p.Name.Equals("containername", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), true,
                    false, false);

                if (container == null)
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container not found";
                }
                else
                {
                    string blobname = string.Empty;
                    Property prp = inputs.Where(p => p.Name.Equals("blobname", StringComparison.OrdinalIgnoreCase)).First();
                    if (prp != null && prp.Value != null)
                    {
                        blobname = prp.Value.ToString();
                    }

                    string path = string.Empty;
                    Property pathProp = inputs.Where(p => p.Name.Equals("directory", StringComparison.OrdinalIgnoreCase)).First();
                    if (pathProp != null && pathProp.Value != null)
                    {
                        path = pathProp.Value.ToString();
                    }

                    path = blobUtils.GetPath(path);

                    blob = blobUtils.UploadBlobFromUrl(container, inputs.Where(p => p.Name.Equals("fileurl", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), path+blobname);

                    if (blob == null)
                    {
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                        returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Upload failed. Blob not found";
                    }
                    else
                    {
                        foreach (Property prop in returns)
                        {
                            switch (prop.Name.ToLower())
                            {
                                case "containername":
                                    prop.Value = container.Name;
                                    break;
                                case "directory":
                                    prop.Value = path;
                                    break;
                                case "blobname":
                                    prop.Value = blob.Name;
                                    break;
                                case "bloburi":
                                    prop.Value = blob.Uri.ToString();
                                    break;
                                case "blobtype":
                                    prop.Value = blob.BlobType.ToString();
                                    break;
                                case "blobparent":
                                    if (blob.Parent != null && blob.Parent.Container != null)
                                    {
                                        prop.Value = blob.Parent.Container.Name;
                                    }
                                    break;
                                case "metadatacount":
                                    prop.Value = blob.Metadata.Count;
                                    break;
                                case "lastmodified":
                                    if (blob.Properties.LastModified.HasValue)
                                    {
                                        prop.Value = blob.Properties.LastModified.Value.DateTime;
                                    }
                                    break;
                                case "blobsize":
                                    prop.Value = blob.Properties.Length;
                                    break;
                                case "leasestate":
                                    prop.Value = blob.Properties.LeaseState;
                                    break;
                                case "leasestatus":
                                    prop.Value = blob.Properties.LeaseStatus;
                                    break;
                                case "contentencoding":
                                    prop.Value = blob.Properties.ContentEncoding;
                                    break;
                                case "contenttype":
                                    prop.Value = blob.Properties.ContentType;
                                    break;
                            }
                        }
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                        //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container deleted";
                    }
                }
            }
            catch (StorageException ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                account = null;
                client = null;
                container = null;
                blob = null;
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }
        
        // this returns all blobs in the container - not scalable. consider updating to container.ListBlobsSegmented()
        public void ExecuteListBlobs(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;

            System.Data.DataRow dr;

            try
            {
                account = blobUtils.GetStorageAccount();
                client = blobUtils.GetBlobClient(account);

                container = blobUtils.GetBlobContainer(client, inputs.Where(p => p.Name.Equals("containername", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), true,
                    false, false);

                if (container == null)
                {
                    dr = serviceBroker.ServicePackage.ResultTable.NewRow();
                    dr["responsestatus"] = ResponseStatus.Error;
                    dr["responsestatusdescription"] = "Container not found";
                    serviceBroker.ServicePackage.ResultTable.Rows.Add(dr);
                }
                else
                {
                    string path = string.Empty;
                    Property prp = inputs.Where(p => p.Name.Equals("directory", StringComparison.OrdinalIgnoreCase)).First();
                    if (prp != null && prp.Value != null)
                    {
                        path = prp.Value.ToString();
                    }

                    path = blobUtils.GetPath(path);

                    bool flat = false;
                    Property prp1 = inputs.Where(p => p.Name.Equals("flattenlist", StringComparison.OrdinalIgnoreCase)).First();
                    if (prp1 != null && prp1.Value != null)
                    {
                        flat = bool.Parse(prp1.Value.ToString());
                    }

                    // update to include BlobListingDetails parameter
                    IEnumerable<IListBlobItem> blobs = container.ListBlobs(path, flat);

                    foreach (IListBlobItem item in container.ListBlobs(path, flat))
                    {
                        if (item.GetType() == typeof(CloudBlockBlob))
                        {
                            CloudBlockBlob blob = (CloudBlockBlob)item;

                            dr = serviceBroker.ServicePackage.ResultTable.NewRow();

                            foreach (Property prop in returns)
                            {
                                switch (prop.Name.ToLower())
                                {
                                    case "containername":
                                        dr[prop.Name] = container.Name;
                                        break;
                                    case "directory":
                                        dr[prop.Name] = path;
                                        break;
                                    case "blobname":
                                        dr[prop.Name] = blob.Name;
                                        break;
                                    case "bloburi":
                                        dr[prop.Name] = blob.Uri.ToString();
                                        break;
                                    case "blobtype":
                                        dr[prop.Name] = blob.BlobType.ToString();
                                        break;
                                    case "blobparent":
                                        if (blob.Parent != null && blob.Parent.Container != null)
                                        {
                                            dr[prop.Name] = blob.Parent.Container.Name;
                                        }
                                        break;
                                    case "metadatacount":
                                        dr[prop.Name] = blob.Metadata.Count;
                                        break;
                                    case "lastmodified":
                                        if (blob.Properties.LastModified.HasValue)
                                        {
                                            dr[prop.Name] = blob.Properties.LastModified.Value.DateTime;
                                        }
                                        break;
                                    case "blobsize":
                                        dr[prop.Name] = blob.Properties.Length;
                                        break;
                                    case "leasestate":
                                        dr[prop.Name] = blob.Properties.LeaseState;
                                        break;
                                    case "leasestatus":
                                        dr[prop.Name] = blob.Properties.LeaseStatus;
                                        break;
                                    case "contentencoding":
                                        dr[prop.Name] = blob.Properties.ContentEncoding;
                                        break;
                                    case "contenttype":
                                        dr[prop.Name] = blob.Properties.ContentType;
                                        break;
                                }
                            }
                            dr["responsestatus"] = ResponseStatus.Success;
                            //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Metadata loaded";
                            serviceBroker.ServicePackage.ResultTable.Rows.Add(dr);

                        }
                        else if (item.GetType() == typeof(CloudPageBlob))
                        {
                            CloudPageBlob blob = (CloudPageBlob)item;

                            dr = serviceBroker.ServicePackage.ResultTable.NewRow();

                            foreach (Property prop in returns)
                            {
                                switch (prop.Name.ToLower())
                                {
                                    case "containername":
                                        dr[prop.Name] = container.Name;
                                        break;
                                    case "blobname":
                                        dr[prop.Name] = blob.Name;
                                        break;
                                    case "bloburi":
                                        dr[prop.Name] = blob.Uri.ToString();
                                        break;
                                    case "blobtype":
                                        dr[prop.Name] = blob.BlobType.ToString();
                                        break;
                                    case "blobparent":
                                        if (blob.Parent != null && blob.Parent.Container != null)
                                        {
                                            dr[prop.Name] = blob.Parent.Container.Name;
                                        }
                                        break;
                                    case "metadatacount":
                                        dr[prop.Name] = blob.Metadata.Count;
                                        break;
                                    case "lastmodified":
                                        if (blob.Properties.LastModified.HasValue)
                                        {
                                            dr[prop.Name] = blob.Properties.LastModified.Value.DateTime;
                                        }
                                        break;
                                    case "blobsize":
                                        dr[prop.Name] = blob.Properties.Length;
                                        break;
                                    case "leasestate":
                                        dr[prop.Name] = blob.Properties.LeaseState;
                                        break;
                                    case "leasestatus":
                                        dr[prop.Name] = blob.Properties.LeaseStatus;
                                        break;
                                    case "contentencoding":
                                        dr[prop.Name] = blob.Properties.ContentEncoding;
                                        break;
                                    case "contenttype":
                                        dr[prop.Name] = blob.Properties.ContentType;
                                        break;
                                }
                            }
                            dr["responsestatus"] = ResponseStatus.Success;
                            //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Metadata loaded";
                            serviceBroker.ServicePackage.ResultTable.Rows.Add(dr);
                        }
                        //else if (item.GetType() == typeof(CloudBlobDirectory))
                        //{
                        //    CloudBlobDirectory directory = (CloudBlobDirectory)item;

                        //    dr = serviceBroker.ServicePackage.ResultTable.NewRow();

                        //    foreach (Property prop in returns)
                        //    {
                        //        switch (prop.Name.ToLower())
                        //        {
                        //            case "containername":
                        //                dr[prop.Name] = container.Name;
                        //                break;
                        //            case "blobname":
                        //                dr[prop.Name] = directory.Container.Name;
                        //                break;
                        //            case "bloburi":
                        //                dr[prop.Name] = directory.Uri;
                        //                break;
                        //            case "blobtype":
                        //                //dr[prop.Name] = directory.
                        //                break;
                        //            case "blobparent":
                        //                if (directory.Parent != null && directory.Parent.Container != null)
                        //                {
                        //                    dr[prop.Name] = directory.Parent.Container.ToString();
                        //                }
                        //                break;
                        //            case "metadatacount":
                        //                //dr[prop.Name] = directory.Metadata.Count;
                        //                break;
                        //        }
                        //    }
                        //    dr["responsestatus"] = ResponseStatus.Success;
                        //    //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Metadata loaded";
                        //    serviceBroker.ServicePackage.ResultTable.Rows.Add(dr);
                        //}
                    }
                }
            }
            catch (StorageException ex)
            {
                dr = serviceBroker.ServicePackage.ResultTable.NewRow();
                dr["responsestatus"] = ResponseStatus.Error;
                dr["responsestatusdescription"] = ex.Message;
                serviceBroker.ServicePackage.ResultTable.Rows.Add(dr);
            }
            catch (Exception ex)
            {
                dr = serviceBroker.ServicePackage.ResultTable.NewRow();
                dr["responsestatus"] = ResponseStatus.Error;
                dr["responsestatusdescription"] = ex.Message;
                serviceBroker.ServicePackage.ResultTable.Rows.Add(dr);
            }
            finally
            {
                account = null;
                client = null;
                container = null;
            }
            //serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteDownloadBlob(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;
            CloudBlockBlob blob;

            try
            {
                account = blobUtils.GetStorageAccount();
                client = blobUtils.GetBlobClient(account);

                container = blobUtils.GetBlobContainer(client, inputs.Where(p => p.Name.Equals("containername", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), true,
                    false, false);

                if (container == null)
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container not found";
                }
                else
                {
                    string blobname = string.Empty;
                    Property prp = inputs.Where(p => p.Name.Equals("blobname", StringComparison.OrdinalIgnoreCase)).First();
                    if (prp != null && prp.Value != null)
                    {
                        blobname = prp.Value.ToString();
                    }

                    blob = blobUtils.GetBlobProperties(container, blobname);

                    if (blob == null)
                    {
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                        returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Upload failed. Blob not found";
                    }
                    else
                    {
                        string filebase64 = string.Empty;
                        using (MemoryStream stream = new MemoryStream())
                        {
                            blob.DownloadToStream(stream);
                            byte[] filebytes = stream.ToArray();

                            filebase64 = Convert.ToBase64String(filebytes);
                        }

                        foreach (Property prop in returns)
                        {
                            switch (prop.Name.ToLower())
                            {
                                case "containername":
                                    prop.Value = container.Name;
                                    break;
                                case "blobname":
                                    prop.Value = blob.Name;
                                    break;
                                case "bloburi":
                                    prop.Value = blob.Uri.ToString();
                                    break;
                                case "blobtype":
                                    prop.Value = blob.BlobType.ToString();
                                    break;
                                case "blobparent":
                                    if (blob.Parent != null && blob.Parent.Container != null)
                                    {
                                        prop.Value = blob.Parent.Container.Name;
                                    }
                                    break;
                                case "metadatacount":
                                    prop.Value = blob.Metadata.Count;
                                    break;
                                case "lastmodified":
                                    if (blob.Properties.LastModified.HasValue)
                                    {
                                        prop.Value = blob.Properties.LastModified.Value.DateTime;
                                    }
                                    break;
                                case "blobsize":
                                    prop.Value = blob.Properties.Length;
                                    break;
                                case "leasestate":
                                    prop.Value = blob.Properties.LeaseState;
                                    break;
                                case "leasestatus":
                                    prop.Value = blob.Properties.LeaseStatus;
                                    break;
                                case "contentencoding":
                                    prop.Value = blob.Properties.ContentEncoding;
                                    break;
                                case "contenttype":
                                    prop.Value = blob.Properties.ContentType;
                                    break;
                                //case "blobfile":
                                //    FileProperty fp = new FileProperty(prop.Name, null, blob.Name, filebase64);
                                //    prop = fp;
                                //    break;
                                //case "filebase64":
                                //    prop.Value = filebase64;
                                //    break;

                            }
                        }

                        // set SoType.File values 
                        ((FileProperty)returns.Where(p => p.Name.Equals("blobfile")).First()).FileName = blob.Name;
                        ((FileProperty)returns.Where(p => p.Name.Equals("blobfile")).First()).Content = filebase64;

                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                        //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container deleted";
                    }
                }
            }
            catch (StorageException ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                account = null;
                client = null;
                container = null;
                blob = null;
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteDownloadBlobAsBase64(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;
            CloudBlockBlob blob;

            try
            {
                account = blobUtils.GetStorageAccount();
                client = blobUtils.GetBlobClient(account);

                container = blobUtils.GetBlobContainer(client, inputs.Where(p => p.Name.Equals("containername", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), true,
                    false, false);

                if (container == null)
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container not found";
                }
                else
                {
                    string blobname = string.Empty;
                    Property prp = inputs.Where(p => p.Name.Equals("blobname", StringComparison.OrdinalIgnoreCase)).First();
                    if (prp != null && prp.Value != null)
                    {
                        blobname = prp.Value.ToString();
                    }

                    blob = blobUtils.GetBlobProperties(container, blobname);

                    if (blob == null)
                    {
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                        returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Upload failed. Blob not found";
                    }
                    else
                    {
                        string filebase64 = string.Empty;
                        using (MemoryStream stream = new MemoryStream())
                        {
                            blob.DownloadToStream(stream);
                            byte[] filebytes = stream.ToArray();

                            filebase64 = Convert.ToBase64String(filebytes);
                        }

                        foreach (Property prop in returns)
                        {
                            switch (prop.Name.ToLower())
                            {
                                case "containername":
                                    prop.Value = container.Name;
                                    break;
                                case "blobname":
                                    prop.Value = blob.Name;
                                    break;
                                case "bloburi":
                                    prop.Value = blob.Uri.ToString();
                                    break;
                                case "blobtype":
                                    prop.Value = blob.BlobType.ToString();
                                    break;
                                case "blobparent":
                                    if (blob.Parent != null && blob.Parent.Container != null)
                                    {
                                        prop.Value = blob.Parent.Container.Name;
                                    }
                                    break;
                                case "metadatacount":
                                    prop.Value = blob.Metadata.Count;
                                    break;
                                //case "blobfile":
                                //    FileProperty fp = new FileProperty(prop.Name, null, blob.Name, filebase64);
                                //    prop = fp;
                                //    break;
                                case "filebase64":
                                    prop.Value = filebase64;
                                    break;
                                case "lastmodified":
                                    if (blob.Properties.LastModified.HasValue)
                                    {
                                        prop.Value = blob.Properties.LastModified.Value.DateTime;
                                    }
                                    break;
                                case "blobsize":
                                    prop.Value = blob.Properties.Length;
                                    break;
                                case "leasestate":
                                    prop.Value = blob.Properties.LeaseState;
                                    break;
                                case "leasestatus":
                                    prop.Value = blob.Properties.LeaseStatus;
                                    break;
                                case "contentencoding":
                                    prop.Value = blob.Properties.ContentEncoding;
                                    break;
                                case "contenttype":
                                    prop.Value = blob.Properties.ContentType;
                                    break;

                            }
                        }
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                        //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container deleted";
                    }
                }
            }
            catch (StorageException ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                account = null;
                client = null;
                container = null;
                blob = null;
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteDownloadBlobToFileSystem(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;
            CloudBlockBlob blob;

            try
            {
                account = blobUtils.GetStorageAccount();
                client = blobUtils.GetBlobClient(account);

                container = blobUtils.GetBlobContainer(client, inputs.Where(p => p.Name.Equals("containername", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), true,
                    false, false);

                if (container == null)
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container not found";
                }
                else
                {
                    string blobname = string.Empty;
                    Property prp = inputs.Where(p => p.Name.Equals("blobname", StringComparison.OrdinalIgnoreCase)).First();
                    if (prp != null && prp.Value != null)
                    {
                        blobname = prp.Value.ToString();
                    }

                    string filepath = string.Empty;
                    Property prp1 = inputs.Where(p => p.Name.Equals("filepath", StringComparison.OrdinalIgnoreCase)).First();
                    if (prp1 != null && prp1.Value != null)
                    {
                        filepath = prp1.Value.ToString();
                    }

                    blob = blobUtils.GetBlobProperties(container, blobname);

                    if (blob == null)
                    {
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                        returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Upload failed. Blob not found";
                    }
                    else
                    {
                        string filebase64 = string.Empty;
                        using (FileStream stream = System.IO.File.OpenWrite(filepath))
                        {
                            blob.DownloadToStream(stream);
                        }

                        foreach (Property prop in returns)
                        {
                            switch (prop.Name.ToLower())
                            {
                                case "containername":
                                    prop.Value = container.Name;
                                    break;
                                case "blobname":
                                    prop.Value = blob.Name;
                                    break;
                                case "bloburi":
                                    prop.Value = blob.Uri.ToString();
                                    break;
                                case "blobtype":
                                    prop.Value = blob.BlobType.ToString();
                                    break;
                                case "blobparent":
                                    if (blob.Parent != null && blob.Parent.Container != null)
                                    {
                                        prop.Value = blob.Parent.Container.Name;
                                    }
                                    break;
                                case "metadatacount":
                                    prop.Value = blob.Metadata.Count;
                                    break;
                                case "filepath":
                                    prop.Value = filepath;
                                    break;
                                case "lastmodified":
                                    if (blob.Properties.LastModified.HasValue)
                                    {
                                        prop.Value = blob.Properties.LastModified.Value.DateTime;
                                    }
                                    break;
                                case "blobsize":
                                    prop.Value = blob.Properties.Length;
                                    break;
                                case "leasestate":
                                    prop.Value = blob.Properties.LeaseState;
                                    break;
                                case "leasestatus":
                                    prop.Value = blob.Properties.LeaseStatus;
                                    break;
                                case "contentencoding":
                                    prop.Value = blob.Properties.ContentEncoding;
                                    break;
                                case "contenttype":
                                    prop.Value = blob.Properties.ContentType;
                                    break;
                            }
                        }
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                        //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container deleted";
                    }
                }
            }
            catch (StorageException ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                account = null;
                client = null;
                container = null;
                blob = null;
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteSetBlobProperties(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;
            CloudBlockBlob blob;

            try
            {
                account = blobUtils.GetStorageAccount();
                client = blobUtils.GetBlobClient(account);

                container = blobUtils.GetBlobContainer(client, inputs.Where(p => p.Name.Equals("containername", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), true,
                    false, false);

                if (container == null)
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container not found";
                }
                else
                {
                    blob = blobUtils.GetBlobProperties(container, inputs.Where(p => p.Name.Equals("blobname", StringComparison.OrdinalIgnoreCase)).First().Value.ToString());

                    if (blob == null)
                    {
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                        returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Blob not found";
                    }
                    else
                    {

                        string ctype = string.Empty;
                        Property prp = inputs.Where(p => p.Name.Equals("contenttype", StringComparison.OrdinalIgnoreCase)).First();
                        if (prp != null && prp.Value != null)
                        {
                            ctype = prp.Value.ToString();
                        }

                        string ccoding = string.Empty;
                        Property prp1 = inputs.Where(p => p.Name.Equals("contentencoding", StringComparison.OrdinalIgnoreCase)).First();
                        if (prp1 != null && prp1.Value != null)
                        {
                            ccoding = prp1.Value.ToString();
                        }

                        if (!string.IsNullOrWhiteSpace(ctype) || !string.IsNullOrWhiteSpace(ccoding))
                        {
                            if(!string.IsNullOrWhiteSpace(ctype))
                            {
                                blob.Properties.ContentType = ctype;
                            }
                            if (!string.IsNullOrWhiteSpace(ccoding))
                            {
                                blob.Properties.ContentEncoding = ccoding;
                            }
                            blob.SetProperties();
                        }

                        foreach (Property prop in returns)
                        {
                            switch (prop.Name.ToLower())
                            {
                                case "containername":
                                    prop.Value = container.Name;
                                    break;
                                case "blobname":
                                    prop.Value = blob.Name;
                                    break;
                                case "bloburi":
                                    prop.Value = blob.Uri.ToString();
                                    break;
                                case "blobtype":
                                    prop.Value = blob.BlobType.ToString();
                                    break;
                                case "blobparent":
                                    if (blob.Parent != null && blob.Parent.Container != null)
                                    {
                                        prop.Value = blob.Parent.Container.Name;
                                    }
                                    break;
                                case "metadatacount":
                                    prop.Value = blob.Metadata.Count;
                                    break;
                                case "lastmodified":
                                    if (blob.Properties.LastModified.HasValue)
                                    {
                                        prop.Value = blob.Properties.LastModified.Value.DateTime;
                                    }
                                    break;
                                case "blobsize":
                                    prop.Value = blob.Properties.Length;
                                    break;
                                case "leasestate":
                                    prop.Value = blob.Properties.LeaseState;
                                    break;
                                case "leasestatus":
                                    prop.Value = blob.Properties.LeaseStatus;
                                    break;
                                case "contentencoding":
                                    prop.Value = blob.Properties.ContentEncoding;
                                    break;
                                case "contenttype":
                                    prop.Value = blob.Properties.ContentType;
                                    break;

                            }
                        }
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                        //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container deleted";
                    }
                }
            }
            catch (StorageException ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                account = null;
                client = null;
                container = null;
                blob = null;
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }



        #endregion Execute
    }
}
