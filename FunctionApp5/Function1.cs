using System;
using CsvHelper.Configuration.Attributes;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Data;

namespace FunctionApp5
{
    public static class Function1
    {
//private static SqlConnection connection = new SqlConnection();
        public class FileUploadedMessage
        {
            public string Email { get; set; }
            public string FileIdentifier { get; set; }
            public string FriendlyName { get; set; }
        }

        [FunctionName("Function1")]
        public static async Task Run([QueueTrigger("summerschoolqueue", Connection = "AzureWebJobsStorage")]FileUploadedMessage message, ILogger log)
        {

            log.LogInformation($"C# Queue trigger function processed: {message.FileIdentifier}");

            string conectionstring = Environment.GetEnvironmentVariable("AzureWebJobsStorage"); 
            var storageAccount = CloudStorageAccount.Parse(conectionstring);
            var blobClient = storageAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference("summerschoolcontainer1");
            var blob = container.GetBlockBlobReference(message.FriendlyName + ".csv");
            
            string line = await blob.DownloadTextAsync();

            string[] users = line.Split(new Char[] { '\n' });

            /* using (MemoryStream blobStream = new MemoryStream())
             {
                 var fail = blob.DownloadToStreamAsync(blobStream);
                 using (var csv = new CsvReader(new StreamReader(blobStream)))
                 {
                     //var len = csv.L;
                     try
                     {

                         IEnumerable<Foo> records = csv.GetRecords<Foo>();
                         log.LogInformation($"Mess: {records.ToArray().Length}");

                     }
                     catch (Exception ex)
                     {
                         log.LogInformation($"Error");
                     }                    
                 }
             }*/

            foreach (string s in users)
            {
                log.LogInformation($"Mes: {s}");

            }

            var str = Environment.GetEnvironmentVariable("sqlbd_connection");
            using (SqlConnection conn = new SqlConnection(str))
            {
                conn.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = conn;
                cmd.CommandText = "INSERT INTO [File] (Email, Url) VALUES (@Email, @Url)";
                cmd.Prepare();

                cmd.Parameters.AddWithValue("@Email", message.Email);
                cmd.Parameters.AddWithValue("@Url", message.FileIdentifier);

                cmd.ExecuteNonQuery();
                string query2 = "Select @@Identity as newId from [File]";
                cmd.CommandText = query2;
                cmd.CommandType = CommandType.Text;
                cmd.Connection = conn;
                
                int newId = Convert.ToInt32(cmd.ExecuteScalar());
                
                for (int i = 0; i < users.Length; i++)
                {
                    InsetDataToDb(users[i], cmd, newId, i);
                }
                log.LogInformation($"Message: {newId}");

                conn.Close();

            }

        }

        public static void InsetDataToDb(string s, SqlCommand cmd, int fileId, int rowId)
        {
            string[] data = s.Split(new Char[] { ';' });
            try
            {
                cmd.CommandText = "INSERT INTO [ProcessedRow] (FileId, FirstName, LastName, Age, RowId) VALUES (@FileId, @FirstName, @LastName, @Age, @RowId)";
                cmd.Prepare();

                cmd.Parameters.AddWithValue("@FileId", fileId);
                cmd.Parameters.AddWithValue("@FirstName", data[0]);
                cmd.Parameters.AddWithValue("@LastName", data[1]);
                cmd.Parameters.AddWithValue("@Age", data[2]);
                cmd.Parameters.AddWithValue("@RowId", rowId);

            }
            catch
            {
                cmd.CommandText = "INSERT INTO [FailedRow] (FileId, RowId, FieldName, FieldValue) VALUES ( @FileId, @RowId, @FieldName, @FieldValue)";
                cmd.Prepare();


                cmd.Parameters.AddWithValue("@FileId", fileId);
                cmd.Parameters.AddWithValue("@RowId", rowId);
                cmd.Parameters.AddWithValue("@FieldName", data[0]);
                cmd.Parameters.AddWithValue("@FieldValue", data[1]);

            }
            finally
            {
                cmd.ExecuteNonQuery();

            }


        }
    }

   

    public class Foo
    {
        [Name("id")]
        public int Id { get; set; }

        [Name("name")]
        public int Name { get; set; }
    }
}
