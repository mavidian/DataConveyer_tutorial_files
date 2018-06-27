using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Orchestrators;
using System;

namespace DataConveyer_tutorial
{
   internal static class Program
   {
      private static void Main()
      {
         Console.WriteLine("Data Conveyer process is starting");

         //Configure Data Conveyer process:
         var config = new OrchestratorConfig()
         {
            InputDataKind = KindOfTextData.Delimited,
            InputFileName = "input.csv",
            HeadersInFirstInputRow = true,
            OutputDataKind = KindOfTextData.Keyword,
            OutputFileName = "output.kw"
         };

         //Execute Data Conveyer process:
         ProcessResult result;
         using (var orchtr = OrchestratorCreator.GetEtlOrchestrator(config))
         {
            result = orchtr.ExecuteAsync().Result;  //wait for completion, i.e. synchronous execution
         }

         //Evaluate completion status:
         switch (result.CompletionStatus)
         {
            case CompletionStatus.IntakeDepleted:
               Console.WriteLine($"Successfully processed {result.RowsWritten.ToString()} records");
               break;
            case CompletionStatus.InitializationError:
               Console.WriteLine("Oops! Initialization error - maybe the input file is missing(?)");
               break;
            default:
               Console.WriteLine($"Oops! Processing resulted in unexpected status of " + result.CompletionStatus.ToString());
               break;
         }

         Console.Write("Press any key to exit...");
         Console.ReadKey();
      }
   }
}
