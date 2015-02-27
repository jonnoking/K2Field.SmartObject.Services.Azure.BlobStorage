using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Microsoft.Win32;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace K2Field.SmartObject.Services.Azure.BlobStorage.Utilities
{
    public class BlobUtils
    {
        public string connectionString = string.Empty;
        public string accountName = string.Empty;
        public string accountKey = string.Empty;

        public BlobUtils(string accountName, string accountKey)
        {
            this.connectionString = string.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}", accountName, accountKey);
        }

        public CloudStorageAccount GetStorageAccount()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(this.connectionString);

            return storageAccount;
        }

        public CloudBlobClient GetBlobClient(CloudStorageAccount storageAccount)
        {
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            return blobClient;
        }

        #region Container

        public CloudBlobContainer GetBlobContainer(CloudBlobClient blobClient, string containerName, bool fetchAttributes = false, bool createIfDoesntExist = false, bool makePublic = false)
        {
            CloudBlobContainer container = blobClient.GetContainerReference(containerName.Replace(" ", "_"));

            if (createIfDoesntExist)
            {
                try
                {
                    // need to be more specific, public container or public blob
                    //if (makePublic)
                    //{
                    //    container.SetPermissions(new BlobContainerPermissions
                    //    {
                    //        PublicAccess = BlobContainerPublicAccessType.Blob
                    //    });
                    //}

                    container.CreateIfNotExists();
                }
                catch (StorageException se)
                {
                    //MessageBox.Show(se.Message);
                    //return null;
                    throw se;
                }
            }
            else
            {
                try
                {
                    if (!container.Exists())
                    {
                        // unlikley to get to this code - will throw 400 bad request
                        //MessageBox.Show("container doesn't exist");
                        return null;
                    }
                }
                catch (StorageException se)
                {
                    // container doesn't exist
                    //MessageBox.Show(se.Message);
                    //return null;
                    throw se;
                }
            }

            if (fetchAttributes)
            {
                container.FetchAttributes();
            }

            return container;
        }

        public CloudBlobContainer CreateContainer(CloudBlobClient blobClient, string containerName, bool makePublic)
        {
            return GetBlobContainer(blobClient, containerName, true);
        }

        public CloudBlobContainer SetContainerPermissions(CloudBlobContainer container, string permission)
        {
            BlobContainerPermissions perm = new BlobContainerPermissions();

            switch (permission.ToLower())
            {
                case "container":
                    perm.PublicAccess = BlobContainerPublicAccessType.Container;
                    break;
                case "blob":
                    perm.PublicAccess = BlobContainerPublicAccessType.Blob;
                    break;
                case "off":
                default:
                    perm.PublicAccess = BlobContainerPublicAccessType.Off;
                    break;
            }
            container.SetPermissions(perm);

            return container;
        }

        public CloudBlobContainer SetContainerPermissions(CloudBlobClient blobClient, string containerName, string permission)
        {
            CloudBlobContainer container = GetBlobContainer(blobClient, containerName);
            return SetContainerPermissions(container, permission);
        }

        public CloudBlobContainer SetContainerMetadata(CloudBlobContainer container, string name, string value)
        {
            container.Metadata[name.Replace(" ", "_")] = value;
            container.SetMetadata();

            return container;
        }

        public Dictionary<string, string> GetContainerMetadata(CloudBlobContainer container)
        {
            container.FetchAttributes();

            return container.Metadata as Dictionary<string, string>;
        }

        public bool DeleteContainer(CloudBlobContainer container)
        {
            bool success = false;
            try
            {
                container.DeleteIfExists();
                success = true;
            }
            catch (Exception ex)
            {
                success = false;
            }

            return success;
        }

        public IEnumerable<IListBlobItem> GetDirectories(CloudBlobContainer container, bool useFlatBlobListing = false, string blobPrefix = "")
        {
            // check for trailing slash
            // add if not there
            // remove if just a /

            if (!string.IsNullOrWhiteSpace(blobPrefix))
            {
                if (blobPrefix.Equals("/"))
                {
                    blobPrefix = "";
                }
                else if (!blobPrefix.EndsWith("/"))
                {
                    blobPrefix += "/";
                }                
            }

            // can't flattern to retrieve directories            
            var blobs = container.ListBlobs(blobPrefix, useFlatBlobListing, BlobListingDetails.None);
            var folders = blobs.Where(b => b as CloudBlobDirectory != null).ToList();
            return folders;
        }

        public CloudBlobDirectory GetDirectory(CloudBlobContainer container, string directory)
        {
            if (!string.IsNullOrWhiteSpace(directory))
            {
                if (directory.Equals("/"))
                {
                    directory = "";
                }
                else if (!directory.EndsWith("/"))
                {
                    directory += "/";
                }
            }
            return container.GetDirectoryReference(directory);
        }

        #endregion Container


        #region Blob


        //public CloudBlockBlob GetBlobReference(CloudBlobContainer container, string blobName)
        //{
        //    CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
        //    if (blob.Exists())
        //    {
        //        //AccessCondition ac = new AccessCondition();
        //        //ac.

        //        //BlobRequestOptions bro = new BlobRequestOptions();
        //        //bro.

        //        //blob.FetchAttributes();
        //        //blob.Uri;
        //    }
        //    //blob.BlobType
        //    //blob.IsSnapshot
        //    //blob.Metadata
        //    //blob.Name
        //    //blob.Parent
        //    //blob.Properties
        //    //blob.SnapshotQualifiedUri
        //    //blob.SnapshotTime
        //    //blob.Uri

        //    return blob;
        //}

        public CloudBlockBlob UploadBlobFromFilesystem(CloudBlobContainer container, string filePath, string path, string blobName = null)
        {
            if (string.IsNullOrWhiteSpace(blobName))
            {
                blobName = System.IO.Path.GetFileName(filePath);
            }

            // Retrieve reference to a blob named "myblob".
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(path + blobName);

            // Create or overwrite the "myblob" blob with contents from a local file.
            using (var fileStream = System.IO.File.OpenRead(filePath))
            {
                blockBlob.UploadFromStream(fileStream);
            }

            // need to add metadata to blob including mimetype, etc
            return blockBlob;
        }

        public CloudBlockBlob UploadBlobFromUrl(CloudBlobContainer container, string url, string path, string blobName = null)
        {
            if (string.IsNullOrWhiteSpace(blobName))
            {
                Uri uri = new Uri(url);
                blobName = System.IO.Path.GetFileName(uri.LocalPath);
            }

            CloudBlockBlob blockBlob = container.GetBlockBlobReference(path + blobName);

            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
            request.Credentials = CredentialCache.DefaultCredentials;
            using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
            {
                blockBlob.UploadFromStream(response.GetResponseStream());
            }

            request = null;

            return blockBlob;
        }

        public CloudBlockBlob UploadBlobFromBase64(CloudBlobContainer container, string base64, string path, string blobName)
        {
            if (string.IsNullOrWhiteSpace(blobName))
            {
                throw new Exception("blobname required for base64 upload");
            }

            CloudBlockBlob blockBlob = container.GetBlockBlobReference(path + blobName);
            System.IO.MemoryStream ms = new System.IO.MemoryStream(Convert.FromBase64String(base64));
            blockBlob.UploadFromStream(ms);

            //blockBlob.UploadFromByteArray(Convert.FromBase64String(base64), 0, Convert.FromBase64String(base64).Length);

            return blockBlob;
        }

        public bool DeleteBlob(CloudBlockBlob blob, string snapshots = "None")
        {
            bool success = false;
            try
            {
                switch (snapshots.ToLower())
                {
                    case "snapshotsonly":
                        blob.DeleteIfExists(DeleteSnapshotsOption.DeleteSnapshotsOnly);
                        break;
                    case "includesnapshots":
                        blob.DeleteIfExists(DeleteSnapshotsOption.IncludeSnapshots);
                        break;
                    case "none":
                    default:
                        blob.DeleteIfExists(DeleteSnapshotsOption.None);
                        break;
                }
                success = true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return success;
        }

        public CloudBlockBlob SetBlobMetadata(CloudBlockBlob blob, string name, string value)
        {
            blob.Metadata[name] = value;
            blob.SetMetadata();
            return blob;
        }

        public CloudBlockBlob SetBlobMetadata(CloudBlobContainer container, string blobName, string name, string value)
        {
            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
            return SetBlobMetadata(blob, name, value);
        }

        public CloudBlockBlob GetBlobProperties(CloudBlobContainer container, string blobName)
        {
            // Retrieve reference to a blob named "myblob".
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

            blockBlob.FetchAttributes();

            // need to add metadata to blob including mimetype, etc
            return blockBlob;
        }

        //public Dictionary<string, Blobs> ListBlobs(CloudBlobContainer container, string path = null, bool flat = false)
        //{
        //    Dictionary<string, Blobs> blobList = new Dictionary<string, Blobs>();

        //    foreach (IListBlobItem item in container.ListBlobs(path, flat))
        //    {
        //        if (item.GetType() == typeof(CloudBlockBlob))
        //        {
        //            CloudBlockBlob blob = (CloudBlockBlob)item;

        //            Blobs b = new Blobs();
        //            b.Type = "CloudBlockBlob";
        //            b.Uri = blob.Uri;
        //            b.Name = blob.Name;
        //            b.Metadata = blob.Metadata as Dictionary<string, string>;
        //            blobList.Add(blob.Name, b);
        //        }
        //        else if (item.GetType() == typeof(CloudPageBlob))
        //        {
        //            CloudPageBlob blob = (CloudPageBlob)item;

        //            Blobs b = new Blobs();
        //            b.Type = "CloudPageBlob";
        //            b.Uri = blob.Uri;
        //            b.Name = blob.Name;
        //            b.Metadata = blob.Metadata as Dictionary<string, string>;
        //            blobList.Add(blob.Name, b);
        //        }
        //        else if (item.GetType() == typeof(CloudBlobDirectory))
        //        {
        //            CloudBlobDirectory directory = (CloudBlobDirectory)item;

        //            Blobs b = new Blobs();
        //            b.Type = "CloudBlobDirectory";
        //            b.Uri = directory.Uri;
        //            b.Name = directory.Prefix;
        //        }
        //    }
        //    return blobList;
        //}


        public string GetPath(string path)
        {
            if (!string.IsNullOrWhiteSpace(path))
            {
                if (path.Equals("/"))
                {
                    path = "";
                }

                if (path.StartsWith("/"))
                {
                    path = path.Remove(0, 1);
                }

                else if (!path.EndsWith("/"))
                {
                    path += "/";
                }
            }
            return path;
        }

        #endregion Blob


        #region File Utils


        public string Serialize(string fileName)
        {
            using (var fileStream = System.IO.File.OpenRead(fileName))
            {
                byte[] buffer = new byte[fileStream.Length];
                fileStream.Read(buffer, 0, (int)fileStream.Length);
                return Convert.ToBase64String(buffer);
            }
        }

        public string GetMimeType(string extension)
        {
            string mimeType = "application/unknown";

            RegistryKey regKey = Registry.ClassesRoot.OpenSubKey(
                extension.ToLower()
                );

            if (regKey != null)
            {
                object contentType = regKey.GetValue("Content Type");

                if (contentType != null)
                    mimeType = contentType.ToString();
            }

            return mimeType;
        }

        public string GetMimeType(FileInfo fileInfo)
        {
            return GetMimeType(fileInfo.Extension.ToLower());
        }


        #endregion File Utils



    }
}
