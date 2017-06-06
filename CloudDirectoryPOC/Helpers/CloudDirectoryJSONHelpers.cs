using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Amazon.CloudDirectory.Model;

namespace CloudDirectoryPOC.Helpers
{
    public static class CloudDirectoryJsonHelpers
    {
        public static T Deserialise<T>(BatchListObjectAttributesResponse ar) where T : class, new()
        {
            T ret = new T();
            var propertyInfo = typeof(T).GetProperties();

            List<string> classProperties = propertyInfo.Select(s => s.Name).ToList();

            var cdProperties = ar.Attributes
                .Select(s => new KeyValuePair<string, string>(s.Key.Name, s.Value.StringValue))
                .ToDictionary(s => s.Key, s2 => s2.Value);

            foreach (var cd in cdProperties)
            {
                if (classProperties.Contains(cd.Key))
                {
                    var spropertyInfo = propertyInfo.First(s => s.Name == cd.Key);
                    spropertyInfo.SetValue(ret, Convert.ChangeType(cd.Value, spropertyInfo.PropertyType), null);
                }
            }

            return ret;
        }

        public static List<AttributeKeyAndValue> Serialise<T>(T item, string facetName, string schemaARN) where T : class, new()
        {
            var propertyInfo = typeof(T).GetProperties();
            var classProperties = propertyInfo.Select(s => new KeyValuePair<string, string>(s.Name, (string)s.GetValue(item))).ToDictionary(s => s.Key, s2 => s2.Value);

            var ret = new List<AttributeKeyAndValue>();

            foreach (var i in classProperties)
            {
                ret.Add(new AttributeKeyAndValue()
                {
                    Key =
                        new AttributeKey() { FacetName = facetName, Name = i.Key, SchemaArn = schemaARN },
                    Value = new TypedAttributeValue() { StringValue = i.Value }
                });
            }

            return ret;
        }
    }
}
