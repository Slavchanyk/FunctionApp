using System;
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

                cmd.ExecuteNonQuery();

                int newId = Convert.ToInt32(cmd.ExecuteScalar());

                log.LogInformation($"Message: {users.Length}");
                for (int i = 0; i < users.Length - 1; i++)
                {
                    SqlCommand command = new SqlCommand();
                    command.Connection = conn;

                    command.Parameters.Add("@FileId", SqlDbType.Int, Int32.MaxValue).Value = newId;
                    command.Parameters.Add("@RowId", SqlDbType.Int, Int32.MaxValue).Value = i + 1;

                    string[] data = users[i].Split(new Char[] { ';' });
                    string q = "";
                    log.LogInformation($"Message: {i}");
                    q = "INSERT INTO [ProcessedRow] (FileId, FirstName, LastName, Age, RowId) VALUES (@FileId, @FirstName, @LastName, @Age, @RowId)";

                    if (data[0].GetType() != typeof(string))
                    {
                        q = " INSERT INTO [FailedRow] (FileId, RowId, FieldName, FieldValue) VALUES ( @FileId, @RowId, @FieldName, @FieldValue)";
                        command.Parameters.Add("@FieldName", SqlDbType.NVarChar, -1).Value = "FirstName";
                        command.Parameters.Add("@FieldValue", SqlDbType.NVarChar, -1).Value = data[0].ToString();
                    }
                    else if (data[1].GetType() != typeof(string))
                    {
                        q = " INSERT INTO [FailedRow] (FileId, RowId, FieldName, FieldValue) VALUES ( @FileId, @RowId, @FieldName, @FieldValue)";
                        command.Parameters.Add("@FieldName", SqlDbType.NVarChar, -1).Value = "LastName";
                        command.Parameters.Add("@FieldValue", SqlDbType.NVarChar, -1).Value = data[1].ToString();
                    }
                    else if (!Int32.TryParse(data[2], out int x))
                    {
                        q = " INSERT INTO [FailedRow] (FileId, RowId, FieldName, FieldValue) VALUES ( @FileId, @RowId, @FieldName, @FieldValue)";
                        command.Parameters.Add("@FieldName", SqlDbType.NVarChar, -1).Value = "Age";
                        command.Parameters.Add("@FieldValue", SqlDbType.NVarChar, -1).Value = data[2].ToString();
                    }
                    else
                    {
                        command.Parameters.Add("@FirstName", SqlDbType.NVarChar, -1).Value = data[0];
                        command.Parameters.Add("@LastName", SqlDbType.NVarChar, -1).Value = data[1];
                        command.Parameters.Add("@Age", SqlDbType.Int, Int32.MaxValue).Value = data[2];
                    }

                    command.CommandText = q;
                    command.Prepare();
                    command.ExecuteNonQuery();

                }
                log.LogInformation($"Message: {newId}");

                conn.Close();

            }

        }
    }
}
