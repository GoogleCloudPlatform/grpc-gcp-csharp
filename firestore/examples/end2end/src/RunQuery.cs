using System;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using Google.Apis.Services;
using Google.Type;
using Google.Cloud.Firestore.Admin.V1Beta1;
using Google.Cloud.Firestore.V1Beta1;
using Grpc.Auth;

namespace FirestoreTest
{
    public partial class FirestoreTestClass
    {
        public async System.Threading.Tasks.Task FSRunQuery()
        {
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\n:: Running a query from Firestore... ::\n");
            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Yellow, "\nUSA Cities with populations larger than 1 millon\n");

            var runQueryRequest = new RunQueryRequest();
            runQueryRequest.Parent = "projects/mch-test-49bba/databases/(default)" + "/documents";
            StructuredQuery sq = new StructuredQuery();

            // just look at cities!
            var colSel = new StructuredQuery.Types.CollectionSelector();
            colSel.CollectionId = "cities";
            sq.From.Add(colSel);

            // only get USA cities with more than 1 million people
            // define USA filter
            StructuredQuery.Types.Filter countryFilter = new StructuredQuery.Types.Filter();
            StructuredQuery.Types.FieldFilter fieldFilter = new StructuredQuery.Types.FieldFilter();
            StructuredQuery.Types.FieldReference field = new StructuredQuery.Types.FieldReference
            {
                FieldPath = "country"
            };
            fieldFilter.Field = field;
            fieldFilter.Op = StructuredQuery.Types.FieldFilter.Types.Operator.Equal;
            Value filterValue = new Value
            {
                StringValue = "USA"
            };
            fieldFilter.Value = filterValue;
            countryFilter.FieldFilter = fieldFilter;
            // define 1 Million filer
            StructuredQuery.Types.Filter populationFilter = new StructuredQuery.Types.Filter();
            fieldFilter = new StructuredQuery.Types.FieldFilter();
            field = new StructuredQuery.Types.FieldReference
            {
                FieldPath = "population"
            };
            fieldFilter.Field = field;
            fieldFilter.Op = StructuredQuery.Types.FieldFilter.Types.Operator.GreaterThanOrEqual;
            filterValue = new Value
            {
                IntegerValue = 1000000
            };
            fieldFilter.Value = filterValue;
            populationFilter.FieldFilter = fieldFilter;

            StructuredQuery.Types.CompositeFilter compositeFilter = new StructuredQuery.Types.CompositeFilter();
            compositeFilter.Filters.Add(countryFilter);
            compositeFilter.Filters.Add(populationFilter);
            compositeFilter.Op = StructuredQuery.Types.CompositeFilter.Types.Operator.And;
            var where = new StructuredQuery.Types.Filter();
            where.CompositeFilter = compositeFilter;
            sq.Where = where;

            runQueryRequest.StructuredQuery = sq;

            var select = new StructuredQuery.Types.Projection();
            var fields = select.Fields;
            var fieldRef = new StructuredQuery.Types.FieldReference
            {
                FieldPath = "name"
            };
            fields.Add(fieldRef);
            fieldRef = new StructuredQuery.Types.FieldReference
            {
                FieldPath = "population"
            };
            fields.Add(fieldRef);

            sq.Select = select;

            var runQueryResponse = new RunQueryResponse();
            try
            {
                
                var ret = FsClient.RunQuery(runQueryRequest);
                var responseStream = ret.ResponseStream;
                var cancellationToken = new System.Threading.CancellationToken();
                while (await responseStream.MoveNext(cancellationToken))
                {
                    runQueryResponse = responseStream.Current;
                    Utils.ReadDocument(runQueryResponse.Document);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("{0} Exception caught.", e);
                return;
            }

            FirestoreTestUtils.ColoredConsoleWrite(ConsoleColor.Green, "\nFinished running query!\n");
        }
    }
}