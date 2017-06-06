using System;
using System.Diagnostics;
using System.Linq;
using Amazon;
using Amazon.CloudDirectory;
using Amazon.Runtime;
using CloudDirectoryPOC.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudDirectoryPOC
{
    [TestClass]
    public class Program
    {
        public static string ItemName = "testitem";
        public static string schemaARN = "arn:aws:clouddirectory:ap-southeast-2:250658028269:directory/ATpvrlVQOk1UqUv8y-8yuy8/schema/person/1";
        public static string directoryARN = "arn:aws:clouddirectory:ap-southeast-2:250658028269:directory/ATpvrlVQOk1UqUv8y-8yuy8";
        public static string facetName = "Person";


        [TestMethod]
        public void Test()
        {
            var cdc = new AmazonCloudDirectoryConfig() { RegionEndpoint = RegionEndpoint.APSoutheast2 };
            AmazonCloudDirectoryClient c = new AmazonCloudDirectoryClient(cred, cdc);



            //Enumerable.Range(0, 10000)
            //    .ToList().AsParallel()
            //    .ForAll(s =>
            //    {
            //        try
            //        {
            //            CloudDirectoryAPI.Delete(c, directoryARN, ItemName + s.ToString()).Wait();
            //        }
            //        catch (Exception e)
            //        {
            //        }

            //    });

            Console.WriteLine("starting 10k write test");
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            CloudDirectoryAPI.CreateBatch(c, schemaARN, directoryARN, facetName).Wait();
            stopwatch.Stop();
            Console.WriteLine($"write 10k in {stopwatch.Elapsed.TotalMilliseconds} MS");

            CloudDirectoryAPI.Delete(c, directoryARN, ItemName).Wait();

            var p1 = CloudDirectoryAPI.Create(c, schemaARN, directoryARN, facetName, ItemName).Result;
            stopwatch = new Stopwatch();

            stopwatch.Start();
            var p2 = CloudDirectoryAPI.Read(c, schemaARN, directoryARN, facetName, ItemName).Result;
            stopwatch.Stop();

            Console.WriteLine($"read 1 in {stopwatch.Elapsed.TotalMilliseconds} MS");

            stopwatch = new Stopwatch();
            stopwatch.Start();
            p2 = CloudDirectoryAPI.Read(c, schemaARN, directoryARN, facetName, ItemName).Result;
            stopwatch.Stop();

            Console.WriteLine($"read 2 in {stopwatch.Elapsed.TotalMilliseconds} MS");

            Assert.AreEqual(p1.username, p2.username);
            Assert.AreEqual(p1.website, p2.website);
        }
    }
}