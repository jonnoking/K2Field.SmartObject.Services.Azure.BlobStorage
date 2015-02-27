using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using SourceCode.SmartObjects.Services.ServiceSDK.Types;
using SourceCode.SmartObjects.Services.ServiceSDK.Objects;
using SourceCode.SmartObjects.Services.ServiceSDK;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace K2Field.SmartObject.Services.Azure.BlobStorage.Data
{
    public class BlobContainer
    {
        private ServiceAssemblyBase serviceBroker = null;

        public BlobContainer(ServiceAssemblyBase serviceBroker)
        {
            // Set local serviceBroker variable.
            this.serviceBroker = serviceBroker;
        }
        /*
         * -- Container --
         * Create container /
         * Read containter details /
         * Set Container Permissions /
         * Read Container Permissions
         * Set Container Metadata /
         * List Container Metadata /
         * Read Container Metadata value /
         * Delete Container /
         *
         * Set Container Properties 
         * List Container Properties 
         * Read Container Properties value 
         */


        #region Describe

        public void Create()
        {
            List<Property> ContainerProps = GetContainerProperties();

            ServiceObject ContainerServiceObject = new ServiceObject();
            ContainerServiceObject.Name = "azurecontainer";
            ContainerServiceObject.MetaData.DisplayName = "Azure Container";
            ContainerServiceObject.MetaData.ServiceProperties.Add("objecttype", "container");
            ContainerServiceObject.Active = true;

            foreach (Property prop in ContainerProps)
            {
                ContainerServiceObject.Properties.Add(prop);
            }
            ContainerServiceObject.Methods.Add(CreateLoadContainer(ContainerProps));
            ContainerServiceObject.Methods.Add(CreateLisContainers(ContainerProps));
            ContainerServiceObject.Methods.Add(CreateCreateContainer(ContainerProps));
            ContainerServiceObject.Methods.Add(CreateSetPermissions(ContainerProps));
            ContainerServiceObject.Methods.Add(CreateDeleteContainer(ContainerProps));
            ContainerServiceObject.Methods.Add(CreateLoadMetadataValue(ContainerProps));
            ContainerServiceObject.Methods.Add(CreateListMetadata(ContainerProps));
            ContainerServiceObject.Methods.Add(CreateSetMetadata(ContainerProps));
            ContainerServiceObject.Methods.Add(CreateListFolders(ContainerProps));

            serviceBroker.Service.ServiceObjects.Add(ContainerServiceObject);

        }

        private List<Property> GetContainerProperties()
        {
            List<Property> ContainerProperties = new List<Property>();

            Property name = new Property {
                Name = "containername",
                MetaData = new MetaData("Container Name", "Container Name"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(name);

            Property uri = new Property
            {
                Name = "containeruri",
                MetaData = new MetaData("Container Uri", "Container Uri"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(uri);

            //Property props = new Property
            //{
            //    Name = "containerproperties",
            //    MetaData = new MetaData("Container Properties", "Container Properties"),
            //    SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Memo,
            //    Type = "string"
            //};
            //ContainerProperties.Add(props);

            //Property metadata = new Property
            //{
            //    Name = "containermetadata",
            //    MetaData = new MetaData("Container Metadata", "Container Metadata"),
            //    SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Memo,
            //    Type = "string"
            //};
            //ContainerProperties.Add(metadata);

            Property metadatakey = new Property
            {
                Name = "containermetadatakey",
                MetaData = new MetaData("Container Metadata Key", "Container Metadata Key"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(metadatakey);

            Property metadatavalue = new Property
            {
                Name = "containermetadatavalue",
                MetaData = new MetaData("Container Metadata Value", "Container Metadata Value"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(metadatavalue);


            Property createifdoesntexist = new Property
            {
                Name = "createifdoesntexist",
                MetaData = new MetaData("Create If Doesnt Exist", "Create If Doesnt Exist"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.YesNo,
                Type = "boolean"
            };
            ContainerProperties.Add(createifdoesntexist);


            Property containerpermissions = new Property
            {
                Name = "containerpermissions",
                MetaData = new MetaData("Container Permissions", "Container Permissions"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(containerpermissions);

            Property itemcount = new Property
            {
                Name = "containeritemcount",
                MetaData = new MetaData("Container Item Count", "Container Item Count"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Number,
                Type = "int"
            };
            ContainerProperties.Add(itemcount);

            Property metadatacount = new Property
            {
                Name = "metadatacount",
                MetaData = new MetaData("Metadata Count", "Metadata Count"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Number,
                Type = "int"
            };
            ContainerProperties.Add(metadatacount);

            Property prefix = new Property
            {
                Name = "prefix",
                MetaData = new MetaData("Prefix", "Prefix"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(prefix);

            Property parent = new Property
            {
                Name = "parent",
                MetaData = new MetaData("Parent", "Parent"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(parent);


            Property directoryname = new Property
            {
                Name = "directoryname",
                MetaData = new MetaData("Directory Name", "Directory Name"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.Text,
                Type = "string"
            };
            ContainerProperties.Add(directoryname);

            Property lastmodified = new Property
            {
                Name = "lastmodified",
                MetaData = new MetaData("Last Modified", "Last Modified"),
                SoType = SourceCode.SmartObjects.Services.ServiceSDK.Types.SoType.DateTime,
                Type = "DateTime"
            };
            ContainerProperties.Add(lastmodified);

            ContainerProperties.AddRange(BlobStandard.GetStandardReturnProperties());

            return ContainerProperties;
        }

        private Method CreateLoadContainer(List<Property> ContainerProps)
        {
            Method LoadContainer = new Method();
            LoadContainer.Name = "loadcontainer";
            LoadContainer.MetaData.DisplayName = "Load Container";
            LoadContainer.Type = MethodType.Read;
            LoadContainer.InputProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            LoadContainer.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            LoadContainer.InputProperties.Add(ContainerProps.Where(p => p.Name == "createifdoesntexist").First());
            //LoadContainer.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "createifdoesntexist").First());

            foreach (Property prop in ContainerProps)
            {
                LoadContainer.ReturnProperties.Add(prop);
            }
            return LoadContainer;
        }

        private Method CreateLisContainers(List<Property> ContainerProps)
        {
            Method CreateListContainers = new Method();
            CreateListContainers.Name = "listcontainers";
            CreateListContainers.MetaData.DisplayName = "List Containers";
            CreateListContainers.Type = MethodType.List;
            CreateListContainers.InputProperties.Add(ContainerProps.Where(p => p.Name == "prefix").First());

            //CreateListContainers.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            //CreateListContainers.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "containermetadatacount").First());
            //CreateListContainers.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "containermetadatavalue").First());
            //CreateListContainers.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "responsestatus").First());
            //CreateListContainers.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "responsestatusdescription").First());

            // need to trim return properties
            foreach (Property prop in ContainerProps)
            {
                CreateListContainers.ReturnProperties.Add(prop);
            }
            return CreateListContainers;
        }

        private Method CreateCreateContainer(List<Property> ContainerProps)
        {
            Method CreateContainer = new Method();
            CreateContainer.Name = "createcontainer";
            CreateContainer.MetaData.DisplayName = "Create Container";
            CreateContainer.Type = MethodType.Read;
            CreateContainer.InputProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            CreateContainer.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            CreateContainer.InputProperties.Add(ContainerProps.Where(p => p.Name == "containerpermissions").First());
            //CreateContainer.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "containerpermissions").First());

            foreach (Property prop in ContainerProps)
            {
                CreateContainer.ReturnProperties.Add(prop);
            }
            return CreateContainer;
        }

        private Method CreateSetPermissions(List<Property> ContainerProps)
        {
            Method CreateSetPermissions = new Method();
            CreateSetPermissions.Name = "setcontainerpermissions";
            CreateSetPermissions.MetaData.DisplayName = "Set Container Permissions";
            CreateSetPermissions.Type = MethodType.Update;
            CreateSetPermissions.InputProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            CreateSetPermissions.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            CreateSetPermissions.InputProperties.Add(ContainerProps.Where(p => p.Name == "containerpermissions").First());
            CreateSetPermissions.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "containerpermissions").First());

            foreach (Property prop in ContainerProps)
            {
                CreateSetPermissions.ReturnProperties.Add(prop);
            }
            return CreateSetPermissions;
        }

        private Method CreateSetMetadata(List<Property> ContainerProps)
        {
            Method CreateSetMetadata = new Method();
            CreateSetMetadata.Name = "setcontainermetadata";
            CreateSetMetadata.MetaData.DisplayName = "Set Container Metadata";
            CreateSetMetadata.Type = MethodType.Update;
            CreateSetMetadata.InputProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            CreateSetMetadata.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            CreateSetMetadata.InputProperties.Add(ContainerProps.Where(p => p.Name == "containermetadatakey").First());
            CreateSetMetadata.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "containermetadatakey").First());
            CreateSetMetadata.InputProperties.Add(ContainerProps.Where(p => p.Name == "containermetadatavalue").First());
            CreateSetMetadata.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "containermetadatavalue").First());

            // need to trim return properties
            foreach (Property prop in ContainerProps)
            {
                CreateSetMetadata.ReturnProperties.Add(prop);
            }
            return CreateSetMetadata;
        }

        private Method CreateLoadMetadataValue(List<Property> ContainerProps)
        {
            Method CreateLoadMetadataValue = new Method();
            CreateLoadMetadataValue.Name = "loadcontainermetadatavalue";
            CreateLoadMetadataValue.MetaData.DisplayName = "Load Container Metadata Value";
            CreateLoadMetadataValue.Type = MethodType.Read;
            CreateLoadMetadataValue.InputProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            CreateLoadMetadataValue.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            CreateLoadMetadataValue.InputProperties.Add(ContainerProps.Where(p => p.Name == "containermetadatakey").First());
            CreateLoadMetadataValue.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "containermetadatakey").First());

            CreateLoadMetadataValue.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            CreateLoadMetadataValue.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "containermetadatakey").First());
            CreateLoadMetadataValue.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "containermetadatavalue").First());
            CreateLoadMetadataValue.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "responsestatus").First());
            CreateLoadMetadataValue.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "responsestatusdescription").First());
            
            // need to trim return properties
            //foreach (Property prop in ContainerProps)
            //{
            //    CreateLoadMetadataValue.ReturnProperties.Add(prop);
            //}
            return CreateLoadMetadataValue;
        }

        private Method CreateListMetadata(List<Property> ContainerProps)
        {
            Method CreateListMetadata = new Method();
            CreateListMetadata.Name = "listcontainermetadata";
            CreateListMetadata.MetaData.DisplayName = "List Container Metadata";
            CreateListMetadata.Type = MethodType.List;
            CreateListMetadata.InputProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            CreateListMetadata.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());

            CreateListMetadata.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            CreateListMetadata.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "containermetadatakey").First());
            CreateListMetadata.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "containermetadatavalue").First());
            CreateListMetadata.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "responsestatus").First());
            CreateListMetadata.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "responsestatusdescription").First());

            // need to trim return properties
            //foreach (Property prop in ContainerProps)
            //{
            //    CreateLoadMetadataValue.ReturnProperties.Add(prop);
            //}
            return CreateListMetadata;
        }

        private Method CreateDeleteContainer(List<Property> ContainerProps)
        {
            Method CreateDeleteContainer = new Method();
            CreateDeleteContainer.Name = "deletecontainer";
            CreateDeleteContainer.MetaData.DisplayName = "Delete Container";
            CreateDeleteContainer.Type = MethodType.Delete;
            CreateDeleteContainer.InputProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            CreateDeleteContainer.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());

            CreateDeleteContainer.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            CreateDeleteContainer.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "responsestatus").First());
            CreateDeleteContainer.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "responsestatusdescription").First());

            // need to trim return properties
            //foreach (Property prop in ContainerProps)
            //{
            //    CreateLoadMetadataValue.ReturnProperties.Add(prop);
            //}
            return CreateDeleteContainer;
        }

        private Method CreateListFolders(List<Property> ContainerProps)
        {
            Method CreateListFolders = new Method();
            CreateListFolders.Name = "listcontainerfolders";
            CreateListFolders.MetaData.DisplayName = "List Container Folders";
            CreateListFolders.Type = MethodType.List;
            CreateListFolders.InputProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            CreateListFolders.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            CreateListFolders.InputProperties.Add(ContainerProps.Where(p => p.Name == "directoryname").First());
            CreateListFolders.Validation.RequiredProperties.Add(ContainerProps.Where(p => p.Name == "directoryname").First());

            CreateListFolders.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "containername").First());
            CreateListFolders.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "directoryname").First());
            CreateListFolders.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "prefix").First());
            CreateListFolders.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "containeritemcount").First());
            CreateListFolders.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "parent").First());
            CreateListFolders.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "responsestatus").First());
            CreateListFolders.ReturnProperties.Add(ContainerProps.Where(p => p.Name == "responsestatusdescription").First());

            // need to trim return properties
            //foreach (Property prop in ContainerProps)
            //{
            //    CreateLoadMetadataValue.ReturnProperties.Add(prop);
            //}
            return CreateListFolders;
        }

        #endregion Describe

        #region Execute


        public void ExecuteLoadContainer(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;

            try
            {
                bool createifdoesntexist = false;
                Property prp = inputs.Where(p => p.Name.Equals("createifdoesntexist", StringComparison.OrdinalIgnoreCase)).First();
                if (prp != null && prp.Value != null)
                {
                    createifdoesntexist = bool.Parse(prp.Value.ToString());
                }

                account = blobUtils.GetStorageAccount();
                client = blobUtils.GetBlobClient(account);

                container = blobUtils.GetBlobContainer(client, inputs.Where(p => p.Name.Equals("containername", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), true,
                    createifdoesntexist, false);

                if (container == null)
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container not found";
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
                            case "containeruri":
                                prop.Value = container.Uri.ToString();
                                break;
                            case "containeritemcount":
                                // could be expensive - consider removing
                                prop.Value = container.ListBlobs(null, true).Count();
                                break;
                            case "metadatacount":
                                prop.Value = container.Metadata.Count;
                                break;
                            case "containerpermissions":
                                BlobContainerPermissions perms = container.GetPermissions();
                                prop.Value = perms.PublicAccess.ToString();
                                break;
                            case "lastmodified":
                                if (container.Properties.LastModified.HasValue)
                                {
                                    prop.Value = container.Properties.LastModified.Value.DateTime;
                                }
                                break;
                        }
                    }
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                    //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container loaded";
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
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteListContainers(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;

            System.Data.DataRow dr;

            try
            {
                account = blobUtils.GetStorageAccount();
                client = blobUtils.GetBlobClient(account);

                // may need refining for performance
                List<CloudBlobContainer> containers = client.ListContainers(string.Empty, ContainerListingDetails.All).ToList();

                foreach (CloudBlobContainer container in containers)
                {
                    dr = serviceBroker.ServicePackage.ResultTable.NewRow();

                    foreach (Property prop in returns)
                    {
                        switch (prop.Name.ToLower())
                        {
                            case "containername":
                                dr[prop.Name] = container.Name;
                                break;
                            case "containeruri":
                                dr[prop.Name] = container.Uri.ToString();
                                break;
                            case "containeritemcount":
                                // could be expensive - consider removing
                                dr[prop.Name] = container.ListBlobs(null, true).Count();
                                break;
                            case "metadatacount":
                                dr[prop.Name] = container.Metadata.Count;
                                break;
                            case "containerpermissions":
                                BlobContainerPermissions perms = container.GetPermissions();
                                dr[prop.Name] = perms.PublicAccess.ToString();
                                break;
                            case "lastmodified":
                                if (container.Properties.LastModified.HasValue)
                                {
                                    dr[prop.Name] = container.Properties.LastModified.Value.DateTime;
                                }
                                break;
                        }
                    }
                    dr["responsestatus"] = ResponseStatus.Success;
                    //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Metadata loaded";
                    serviceBroker.ServicePackage.ResultTable.Rows.Add(dr);
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
            }
            //serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteCreateContainer(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;

            try
            {
                string permissions = string.Empty;
                Property prp = inputs.Where(p => p.Name.Equals("containerpermissions", StringComparison.OrdinalIgnoreCase)).First();
                if (prp != null && prp.Value != null)
                {
                    permissions = prp.Value.ToString();
                }

                account = blobUtils.GetStorageAccount();
                client = blobUtils.GetBlobClient(account);

                container = blobUtils.GetBlobContainer(client, inputs.Where(p => p.Name.Equals("containername", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), true,
                    true, false);

                if (container == null)
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container not found";
                }
                else
                {

                    container = blobUtils.SetContainerPermissions(container, permissions);

                    foreach (Property prop in returns)
                    {
                        switch (prop.Name.ToLower())
                        {
                            case "containername":
                                prop.Value = container.Name;
                                break;
                            case "containeruri":
                                prop.Value = container.Uri.ToString();
                                break;
                            case "containeritemcount":
                                // could be expensive - consider removing
                                prop.Value = container.ListBlobs(null, true).Count();
                                break;
                            case "metadatacount":
                                prop.Value = container.Metadata.Count;
                                break;
                            case "containerpermissions":
                                BlobContainerPermissions perms = container.GetPermissions();
                                prop.Value = perms.PublicAccess.ToString();
                                break;
                            case "lastmodified":
                                if (container.Properties.LastModified.HasValue)
                                {
                                    prop.Value = container.Properties.LastModified.Value.DateTime;
                                }
                                break;
                        }
                    }
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                    //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container loaded";
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
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteSetPermissions(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;

            try
            {
                string permissions = string.Empty;
                Property prp = inputs.Where(p => p.Name.Equals("containerpermissions", StringComparison.OrdinalIgnoreCase)).First();
                if (prp != null && prp.Value != null)
                {
                    permissions = prp.Value.ToString();
                }

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
                    container = blobUtils.SetContainerPermissions(container, permissions);

                    foreach (Property prop in returns)
                    {
                        switch (prop.Name.ToLower())
                        {
                            case "containername":
                                prop.Value = container.Name;
                                break;
                            case "containeruri":
                                prop.Value = container.Uri.ToString();
                                break;
                            case "containeritemcount":
                                // could be expensive - consider removing
                                prop.Value = container.ListBlobs(null, true).Count();
                                break;
                            case "metadatacount":
                                prop.Value = container.Metadata.Count;
                                break;
                            case "containerpermissions":
                                BlobContainerPermissions perms = container.GetPermissions();
                                prop.Value = perms.PublicAccess.ToString();
                                break;
                            case "lastmodified":
                                if (container.Properties.LastModified.HasValue)
                                {
                                    prop.Value = container.Properties.LastModified.Value.DateTime;
                                }
                                break;
                        }
                    }
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container loaded";
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
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteSetMetadata(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;

            try
            {
                string mdkey = string.Empty;
                Property prp = inputs.Where(p => p.Name.Equals("containermetadatakey", StringComparison.OrdinalIgnoreCase)).First();
                if (prp != null && prp.Value != null)
                {
                    mdkey = prp.Value.ToString();
                }

                string mdvalue = string.Empty;
                Property prp1 = inputs.Where(p => p.Name.Equals("containermetadatavalue", StringComparison.OrdinalIgnoreCase)).First();
                if (prp1 != null && prp1.Value != null)
                {
                    mdvalue = prp1.Value.ToString();
                }

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
                    container = blobUtils.SetContainerMetadata(container, mdkey, mdvalue);

                    foreach (Property prop in returns)
                    {
                        switch (prop.Name.ToLower())
                        {
                            case "containername":
                                prop.Value = container.Name;
                                break;
                            case "containeruri":
                                prop.Value = container.Uri.ToString();
                                break;
                            case "containeritemcount":
                                // could be expensive - consider removing
                                prop.Value = container.ListBlobs(null, true).Count();
                                break;
                            case "metadatacount":
                                prop.Value = container.Metadata.Count;
                                break;
                            case "containerpermissions":
                                BlobContainerPermissions perms = container.GetPermissions();
                                prop.Value = perms.PublicAccess.ToString();
                                break;
                            case "containermetadatakey":
                                prop.Value = mdkey;
                                break;
                            case "containermetadatavalue":
                                prop.Value = mdvalue;
                                break;
                            case "lastmodified":
                                if (container.Properties.LastModified.HasValue)
                                {
                                    prop.Value = container.Properties.LastModified.Value.DateTime;
                                }
                                break; 
                        }
                    }
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                    //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container loaded";
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
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteLoadMetadataValue(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;

            try
            {
                string mdkey = string.Empty;
                Property prp = inputs.Where(p => p.Name.Equals("containermetadatakey", StringComparison.OrdinalIgnoreCase)).First();
                if (prp != null && prp.Value != null)
                {
                    mdkey = prp.Value.ToString();
                }

                string mdvalue = string.Empty;

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
                    Dictionary<string, string> md = blobUtils.GetContainerMetadata(container);

                    mdvalue = md[mdkey];

                    foreach (Property prop in returns)
                    {
                        switch (prop.Name.ToLower())
                        {
                            case "containername":
                                prop.Value = container.Name;
                                break;
                            case "containeruri":
                                prop.Value = container.Uri.ToString();
                                break;
                            case "containeritemcount":
                                // could be expensive - consider removing
                                prop.Value = container.ListBlobs(null, true).Count();
                                break;
                            case "metadatacount":
                                prop.Value = container.Metadata.Count;
                                break;
                            case "containerpermissions":
                                BlobContainerPermissions perms = container.GetPermissions();
                                prop.Value = perms.PublicAccess.ToString();
                                break;
                            case "containermetadatakey":
                                prop.Value = mdkey;
                                break;
                            case "containermetadatavalue":
                                prop.Value = mdvalue;
                                break;
                            case "lastmodified":
                                if (container.Properties.LastModified.HasValue)
                                {
                                    prop.Value = container.Properties.LastModified.Value.DateTime;
                                }
                                break;
                        }
                    }
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                    //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container loaded";
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
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteDeleteContainer(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.BlobUtils blobUtils = new Utilities.BlobUtils(serviceBroker.Service.ServiceConfiguration["AccountName"].ToString(), serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString());

            serviceObject.Properties.InitResultTable();

            CloudStorageAccount account;
            CloudBlobClient client;
            CloudBlobContainer container;

            try
            {
                account = blobUtils.GetStorageAccount();
                client = blobUtils.GetBlobClient(account);

                container = blobUtils.GetBlobContainer(client, inputs.Where(p => p.Name.Equals("containername", StringComparison.OrdinalIgnoreCase)).First().Value.ToString(), false,
                    false, false);

                if (container == null)
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container not found";
                }
                else
                {
                    bool success = blobUtils.DeleteContainer(container);

                    foreach (Property prop in returns)
                    {
                        switch (prop.Name.ToLower())
                        {
                            case "containername":
                                prop.Value = container.Name;
                                break;
                        }
                    }
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                    //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Container deleted";
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
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ExecuteListMetadata(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
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
                    Dictionary<string, string> md = blobUtils.GetContainerMetadata(container);
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
                                case "containermetadatakey":
                                    dr[prop.Name] = data.Key;
                                    break;
                                case "containermetadatavalue":
                                    dr[prop.Name] = data.Value;
                                    break;
                            }
                        }
                        dr["responsestatus"] = ResponseStatus.Success;
                        //returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Metadata loaded";
                        serviceBroker.ServicePackage.ResultTable.Rows.Add(dr);
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

        public void ExecuteListContainerFolders(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
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
                    foreach (IListBlobItem item in blobUtils.GetDirectories(container, false, inputs.Where(p => p.Name.Equals("directoryname", StringComparison.OrdinalIgnoreCase)).First().Value.ToString()))
                    {
                        if (item.GetType() == typeof(CloudBlobDirectory))
                        {
                            CloudBlobDirectory directory = (CloudBlobDirectory)item;

                            dr = serviceBroker.ServicePackage.ResultTable.NewRow();

                            foreach (Property prop in returns)
                            {
                                switch (prop.Name.ToLower())
                                {
                                    case "containername":
                                        dr[prop.Name] = container.Name;
                                        break;
                                    case "directoryname":
                                        dr[prop.Name] = inputs.Where(p => p.Name.Equals("directoryname", StringComparison.OrdinalIgnoreCase)).First().Value.ToString();
                                        break;
                                    case "prefix":
                                        dr[prop.Name] = directory.Prefix;
                                        break;
                                    case "bloburi":
                                        dr[prop.Name] = directory.Uri;
                                        break;
                                    case "blobtype":
                                        //dr[prop.Name] = directory.
                                        break;
                                    case "parent":
                                        if (directory.Parent != null && directory.Parent.Container != null)
                                        {
                                            dr[prop.Name] = directory.Parent.Container.Name;
                                        }
                                        break;
                                    case "containeritemcount":
                                        // cloud be expensive - may be unncessary
                                        dr[prop.Name] = directory.ListBlobs(false).Count();
                                        break;
                                }
                            }
                            dr["responsestatus"] = ResponseStatus.Success;
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
            }
            //serviceObject.Properties.BindPropertiesToResultTable();
        }


        #endregion Execute
    }
}
