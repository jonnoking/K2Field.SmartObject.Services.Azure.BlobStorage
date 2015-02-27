using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Xml;
using System.Linq;
//using System.Xml.Linq;

using SourceCode.SmartObjects.Services.ServiceSDK;
using SourceCode.SmartObjects.Services.ServiceSDK.Objects;
using SourceCode.SmartObjects.Services.ServiceSDK.Types;

using K2Field.SmartObject.Services.Azure.BlobStorage.Interfaces;

namespace K2Field.SmartObject.Services.Azure.BlobStorage.Data
{
    /// <summary>
    /// A concrete implementation of IDataConnector responsible for interacting with an underlying system or technology. The purpose of this class it to expose and represent the underlying data and services as Service Objects for consumptions by K2 SmartObjects.
    /// </summary>
    class DataConnector : IDataConnector
    {
        #region Class Level Fields

        #region Constants
        /// <summary>
        /// Constant for the Type Mappings configuration lookup in the service instance.
        /// </summary>
        private static string __TypeMappings = "Type Mappings";
        #endregion

        #region Private Fields
        /// <summary>
        /// Local serviceBroker variable.
        /// </summary>
        private ServiceAssemblyBase serviceBroker = null;
        private string accountName = string.Empty;
        private string accountKey = string.Empty;
        private string connectionString = string.Empty;

        #endregion

        #endregion

        #region Constructor
        /// <summary>
        /// Instantiates a new DataConnector.
        /// </summary>
        /// <param name="serviceBroker">The ServiceBroker.</param>
        public DataConnector(ServiceAssemblyBase serviceBroker)
        {
            // Set local serviceBroker variable.
            this.serviceBroker = serviceBroker;
        }
        #endregion

        #region Methods

        #region void Dispose()
        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            // Add any additional IDisposable implementation code here. Make sure to dispose of any data connections.
            // Clear references to serviceBroker.
            serviceBroker = null;
        }
        #endregion

        #region void GetConfiguration()
        /// <summary>
        /// Gets the configuration from the service instance and stores the retrieved configuration in local variables for later use.
        /// </summary>
        public void GetConfiguration()
        {
            accountName = serviceBroker.Service.ServiceConfiguration["AccountName"].ToString();
            accountKey = serviceBroker.Service.ServiceConfiguration["AccountKey"].ToString();
        }
        #endregion

        #region void SetupConfiguration()
        /// <summary>
        /// Sets up the required configuration parameters in the service instance. When a new service instance is registered for this ServiceBroker, the configuration parameters are surfaced to the appropriate tooling. The configuration parameters are provided by the person registering the service instance.
        /// </summary>
        public void SetupConfiguration()
        {
            serviceBroker.Service.ServiceConfiguration.Add("AccountName", true, "");
            serviceBroker.Service.ServiceConfiguration.Add("AccountKey", true, "");

        }
        #endregion

        #region void SetupService()
        /// <summary>
        /// Sets up the service instance's default name, display name, and description.
        /// </summary>
        public void SetupService()
        {
            serviceBroker.Service.Name = "AzureBlobStorage"+accountName;
            serviceBroker.Service.MetaData.DisplayName = "Azure Blob Storage - " + accountName;
            serviceBroker.Service.MetaData.Description = "Azure Blob Storage - " + accountName;
        }
        #endregion

        #region void DescribeSchema()
        /// <summary>
        /// Describes the schema of the underlying data and services to the K2 platform.
        /// </summary>
        public void DescribeSchema()
        {
            TypeMappings map = GetTypeMappings();

            BlobContainer container = new BlobContainer(serviceBroker);
            container.Create();

            BlobBlob blob = new BlobBlob(serviceBroker);
            blob.Create();

        }

        #endregion


        #region XmlDocument DiscoverSchema()
        /// <summary>
        /// Discovers the schema of the underlying data and services, and then maps the schema into a structure and format which is compliant with the requirements of Service Objects.
        /// </summary>
        /// <returns>An XmlDocument containing the discovered schema in a structure which complies with the requirements of Service Objects.</returns>
        public XmlDocument DiscoverSchema()
        {            
            return null;
        }
        #endregion

        #region TypeMappings GetTypeMappings()
        /// <summary>
        /// Gets the type mappings used to map the underlying data's types to the appropriate K2 SmartObject types.
        /// </summary>
        /// <returns>A TypeMappings object containing the ServiceBroker's type mappings which were previously stored in the service instance configuration.</returns>
        public TypeMappings GetTypeMappings()
        {
            // Lookup and return the type mappings stored in the service instance.
            return (TypeMappings)serviceBroker.Service.ServiceConfiguration[__TypeMappings];
        }
        #endregion

        #region void SetTypeMappings()
        /// <summary>
        /// Sets the type mappings used to map the underlying data's types to the appropriate K2 SmartObject types.
        /// </summary>
        public void SetTypeMappings()
        {
            // Variable declaration.
            TypeMappings map = new TypeMappings();

            // Add type mappings.
            

            // Add the type mappings to the service instance.
            serviceBroker.Service.ServiceConfiguration.Add(__TypeMappings, map);
        }
        #endregion

        #region void Execute(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        /// <summary>
        /// Executes the Service Object method and returns any data.
        /// </summary>
        /// <param name="inputs">A Property[] array containing all the allowed input properties.</param>
        /// <param name="required">A RequiredProperties collection containing the required properties.</param>
        /// <param name="returns">A Property[] array containing all the allowed return properties.</param>
        /// <param name="methodType">A MethoType indicating what type of Service Object method was called.</param>
        /// <param name="serviceObject">A ServiceObject containing populated properties for use with the method call.</param>
        public void Execute(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            #region Container


            if (serviceObject.Methods[0].Name.Equals("loadcontainer"))
            {
                BlobContainer container = new BlobContainer(serviceBroker);
                container.ExecuteLoadContainer(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("listcontainers"))
            {
                BlobContainer container = new BlobContainer(serviceBroker);
                container.ExecuteListContainers(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("createcontainer"))
            {
                BlobContainer container = new BlobContainer(serviceBroker);
                container.ExecuteCreateContainer(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("setcontainerpermissions"))
            {
                BlobContainer container = new BlobContainer(serviceBroker);
                container.ExecuteSetPermissions(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("setcontainermetadata"))
            {
                BlobContainer container = new BlobContainer(serviceBroker);
                container.ExecuteSetMetadata(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("loadcontainermetadatavalue"))
            {
                BlobContainer container = new BlobContainer(serviceBroker);
                container.ExecuteLoadMetadataValue(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("listcontainermetadata"))
            {
                BlobContainer container = new BlobContainer(serviceBroker);
                container.ExecuteListMetadata(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("deletecontainer"))
            {
                BlobContainer container = new BlobContainer(serviceBroker);
                container.ExecuteDeleteContainer(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("listcontainerfolders"))
            {
                BlobContainer container = new BlobContainer(serviceBroker);
                container.ExecuteListContainerFolders(inputs, required, returns, methodType, serviceObject);
            }

            #endregion Container


            #region Blob


            if (serviceObject.Methods[0].Name.Equals("loadblob"))
            {
                BlobBlob blob = new BlobBlob(serviceBroker);
                blob.ExecuteLoadBlob(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("setblobmetadata"))
            {
                BlobBlob blob = new BlobBlob(serviceBroker);
                blob.ExecuteSetBlobMetadata(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("loadblobmetadatavalue"))
            {
                BlobBlob blob = new BlobBlob(serviceBroker);
                blob.ExecuteLoadBlobMetadataValue(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("listblobmetadata"))
            {
                BlobBlob blob = new BlobBlob(serviceBroker);
                blob.ExecuteListBlobMetadata(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("deleteblob"))
            {
                BlobBlob blob = new BlobBlob(serviceBroker);
                blob.ExecuteDeleteBlob(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("uploadblob"))
            {
                BlobBlob blob = new BlobBlob(serviceBroker);
                blob.ExecuteUploadBlob(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("uploadblobfrombase64"))
            {
                BlobBlob blob = new BlobBlob(serviceBroker);
                blob.ExecuteUploadBlobFromBase64(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("uploadblobfromfilesystem"))
            {
                BlobBlob blob = new BlobBlob(serviceBroker);
                blob.ExecuteUploadBlobFromFilePath(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("uploadblobfromurl"))
            {
                BlobBlob blob = new BlobBlob(serviceBroker);
                blob.ExecuteUploadBlobFromUrl(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("listblobs"))
            {
                BlobBlob blob = new BlobBlob(serviceBroker);
                blob.ExecuteListBlobs(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("downloadblob"))
            {
                BlobBlob blob = new BlobBlob(serviceBroker);
                blob.ExecuteDownloadBlob(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("downloadblobtobase64"))
            {
                BlobBlob blob = new BlobBlob(serviceBroker);
                blob.ExecuteDownloadBlobAsBase64(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("downloadblobtofilesystem"))
            {
                BlobBlob blob = new BlobBlob(serviceBroker);
                blob.ExecuteDownloadBlobToFileSystem(inputs, required, returns, methodType, serviceObject);
            }

            if (serviceObject.Methods[0].Name.Equals("setblobproperties"))
            {
                BlobBlob blob = new BlobBlob(serviceBroker);
                blob.ExecuteSetBlobProperties(inputs, required, returns, methodType, serviceObject);
            }

            #endregion Blob

        }
        #endregion

        #endregion
    }
}