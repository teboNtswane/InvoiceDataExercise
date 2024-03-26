using InvoiceDataExercise.bin.Debug;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity.Core.Common.CommandTrees.ExpressionBuilder;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.Remoting.Contexts;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Threading.Tasks;

namespace InvoiceDataExercise
{
    class Program
    {

        //---------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// This demo console application imports the data from the attached data.csv (located in the bin-Debug folder) file into a MS SQL database, using Enity Framework. The total Quantity and the Sum of The Invoice quantity and Unit Selling Price will be displayed to the user when the data from the csv has been imported successfully.
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            string connectionString = @"data source=(localdb)\MSSQLLocalDB;initial catalog=InvoiceImport;integrated security=True;MultipleActiveResultSets=True;App=EntityFramework";
            string dataCsvFile = @"data.csv";

            //Stores data from csv into dataTable to be accessed
            DataTable displayData = new DataTable();

            try
            {
                using (var dbContext = new InvoiceImportEntities())
                {

                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        connection.Open();

                        using (StreamReader reader = new StreamReader(dataCsvFile))
                        {


                            while (!reader.EndOfStream)
                            {
                                string[] headers = reader.ReadLine().Split(',');

                                foreach (string header in headers)
                                {
                                    displayData.Columns.Add(header);
                                }

                                while (!reader.EndOfStream)
                                {
                                    string[] rows = reader.ReadLine().Split(',');
                                    DataRow row = displayData.NewRow();
                                    for (int i = 0; i < headers.Length; i++)
                                    {
                                        row[i] = rows[i];
                                    }
                                    displayData.Rows.Add(row);

                                }
                            }

                        }

                        //This removes the duplicates in the InvoiceHeaders table
                        var distinctData = dbContext.InvoiceHeaders
                       .Select(x => new { x.InvoiceNumber, x.InvoiceDate, x.Address, x.InvoiceTotal })
                       .Distinct()
                       .ToList();
                        //-----> Commented out due to compiler error <-------
                        //This clears the existing data and adds the distinct data
                        //dbContext.InvoiceHeaders.RemoveRange(dbContext.InvoiceHeaders);
                        //dbContext.InvoiceHeaders.AddRange(distinctData);
                        //dbContext.SaveChanges();

                        //Inserts the respective data into the Invoice Headers Table
                        foreach (DataRow row in displayData.Rows)
                        {
                            string insertInvoiceHeader = "INSERT INTO InvoiceHeader (InvoiceNumber, InvoiceDate, Address, InvoiceTotal) VALUES (@InvoiceNumber, @InvoiceDate, @Address, @InvoiceTotal)";

                            using (SqlCommand command = new SqlCommand(insertInvoiceHeader, connection))
                            {
                                string dateFormat = "dd/MM/yyyy HH:mm"; // Specifies the date format in the CSV file

                                command.Parameters.AddWithValue("@InvoiceNumber", row["Invoice Number"]);
                                command.Parameters.AddWithValue("@InvoiceDate", DateTime.ParseExact((string)row["Invoice Date"], dateFormat, CultureInfo.InvariantCulture));
                                command.Parameters.AddWithValue("@Address", row["Address"]);
                                command.Parameters.AddWithValue("@InvoiceTotal", row["Invoice Total Ex VAT"]);

                                command.ExecuteNonQuery();
                            }

                        }

                        //Inserts the respective data into the Invoice Lines Table
                        foreach (DataRow line in displayData.Rows)
                        {
                            string insertInvoiceLine = "INSERT INTO InvoiceLines (InvoiceNumber, Description, Quantity, UnitSellingPriceExVAT) VALUES  (@InvoiceNumber, @Description, @Quantity, @UnitSellingPriceExVAT)";

                            using (SqlCommand command = new SqlCommand(insertInvoiceLine, connection))
                            {
                                command.Parameters.AddWithValue("@InvoiceNumber", line["Invoice Number"]);
                                command.Parameters.AddWithValue("@Description", line["Line Description"]);
                                command.Parameters.AddWithValue("@Quantity", line["Invoice Quantity"]);
                                command.Parameters.AddWithValue("@UnitSellingPriceExVAT", line["Unit selling price ex VAT"]);

                                command.ExecuteNonQuery();
                            }

                        }
                        
                        Console.WriteLine("Data Import Successful.");
                        Console.WriteLine("---------------------------------------------------------------");

                        //Invoice number and Total Quantity of all invoices in the InvoiceLines Table
                        var resultData = dbContext.InvoiceLines
                            .GroupBy(x => x.InvoiceNumber)
                            .Select(y => new
                            {
                                InvoiceNumber = y.Key,
                                total = y.Sum(i => i.Quantity)
                            });
                        foreach (var group in resultData)
                        {
                            Console.WriteLine($"Invoice Number = {group.InvoiceNumber} Total Quantity = {group.total}");
                        }
                        Console.WriteLine("---------------------------------------------------------------");
                        var totalSum = dbContext.InvoiceLines.Sum(i => i.Quantity * i.UnitSellingPriceExVAT);
                        Console.WriteLine($"Total Sum for Invoice Lines = {totalSum}");
                        Console.WriteLine("---------------------------------------------------------------");
                        connection.Close();
                    }
                    dbContext.SaveChanges();
                }

                Console.ReadLine();
            }
            catch(Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }

        }

    }
}
///-------------------------------------------->000ooo...END OF FILE...ooo000<---------------------------------------------