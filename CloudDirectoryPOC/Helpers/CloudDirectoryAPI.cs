using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.CloudDirectory;
using Amazon.CloudDirectory.Model;
using Newtonsoft.Json;

namespace CloudDirectoryPOC.Helpers
{
    public static class CloudDirectoryAPI
    {
        public static async Task Delete(AmazonCloudDirectoryClient c, string directoryARN, string itemName)
        {
            //cant delete without detatching from node
            var det = new DetachObjectRequest()
            {
                DirectoryArn = directoryARN,
                LinkName = itemName,
                ParentReference = new ObjectReference() { Selector = "/" }
            };

            string refd = null;
            try
            {
                var detr = c.DetachObjectAsync(det).Result;
                refd = detr.DetachedObjectIdentifier;
            }
            //first run
            catch (Exception e)
            {
            }

            //already deleted?
            if (refd == null)
                return;

            var dor = new DeleteObjectRequest()
            {
                DirectoryArn = directoryARN,
                ObjectReference = new ObjectReference()
                {
                    //absolute ref to the detached item
                    Selector = "$" + refd
                }
            };

            try
            {
                await c.DeleteObjectAsync(dor);
            }
            //first run
            catch (Exception e)
            {
            }
        }

        public static async Task CreateBatch(AmazonCloudDirectoryClient c, string schemaARN, string directoryARN,
            string facetName)
        {
            var tasks =
            Enumerable.Range(0, 10)
                .ToList()
                .Select(s => CreateBatchAux(c, schemaARN, directoryARN, facetName)).ToArray();

            Task.WaitAll(tasks);
        }

        private static async Task CreateBatchAux(AmazonCloudDirectoryClient c, string schemaARN, string directoryARN,
            string facetName)
        {
            var oal = CloudDirectoryJsonHelpers.Serialise(
                new Person() { username = "test batch username", website = "test batch website" }, facetName, schemaARN);

            var createbatch = new ConcurrentBag<BatchWriteOperation>();

            Enumerable.Range(0, 10)
                .ToList()
                .ForEach(s =>
                {
                    var r = new Random((int)DateTime.UtcNow.Ticks);
                    createbatch.Add(new BatchWriteOperation()
                    {
                        CreateObject = new BatchCreateObject()
                        {
                            SchemaFacet =
                                new List<SchemaFacet>()
                                {
                                    new SchemaFacet() {FacetName = facetName, SchemaArn = schemaARN}
                                },
                            ObjectAttributeList = oal,
                            BatchReferenceName = "brn" + r.Next(),
                            LinkName = "batchitems" + r.Next().ToString(),
                            ParentReference = new ObjectReference() { Selector = "/" }
                        }
                    });
                });


            await c.BatchWriteAsync(new BatchWriteRequest() { Operations = createbatch.ToList(), DirectoryArn = directoryARN });
        }

        public static async Task<Person> Create(AmazonCloudDirectoryClient c, string schemaARN, string directoryARN, string facetName, string itemName)
        {
            var facets = await c.ListFacetNamesAsync(new ListFacetNamesRequest()
            {
                SchemaArn = schemaARN
            });
            Console.WriteLine("facets:" + JsonConvert.SerializeObject(facets));


            var lpp = await c.ListObjectParentPathsAsync(new ListObjectParentPathsRequest()
            {
                DirectoryArn = directoryARN,
                ObjectReference = new ObjectReference() { Selector = "/" }
            });

            Console.WriteLine("parent paths:" + JsonConvert.SerializeObject(lpp));


            var cor = new CreateObjectRequest();
            cor.DirectoryArn = directoryARN;

            var p = new Person() { username = "test username", website = "test website" };

            var oal = CloudDirectoryJsonHelpers.Serialise(p, facetName, schemaARN);

            cor.ObjectAttributeList = oal;

            cor.ParentReference = new ObjectReference() { Selector = "/" };
            // [^\/\[\]\(\):\{\}#@!?\s\\;]+)
            cor.LinkName = itemName;


            cor.SchemaFacets = new List<SchemaFacet>()
            {
                new SchemaFacet()
                {
                    FacetName =facetName,
                    SchemaArn =
                        schemaARN
                }
            };

            await c.CreateObjectAsync(cor);
            return p;
        }

        public static async Task<Person> Read(AmazonCloudDirectoryClient c, string schemaARN, string directoryARN,
            string facetName, string itemName)
        {

            var brr = new BatchReadRequest()
            {
                ConsistencyLevel = ConsistencyLevel.SERIALIZABLE,
                DirectoryArn = directoryARN,
                Operations = new List<BatchReadOperation>()
                {
                    new BatchReadOperation()
                    {
                        ListObjectAttributes = new BatchListObjectAttributes()
                        {
                            FacetFilter = new SchemaFacet()
                            {
                                SchemaArn = schemaARN,
                                FacetName = facetName
                            },
                            ObjectReference = new ObjectReference {Selector = "/"+itemName}
                        }
                    }
                }
            };

            try
            {
                Stopwatch s = new Stopwatch();
                s.Start();
                var res = await c.BatchReadAsync(brr);
                s.Stop();

                var user = CloudDirectoryJsonHelpers.Deserialise<Person>(res.Responses[0].SuccessfulResponse.ListObjectAttributes);
                return user;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
    }
}
