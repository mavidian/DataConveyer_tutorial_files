using Mavidian.DataConveyer;
using Mavidian.DataConveyer.Entities.KeyVal;
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
            AllowTransformToAlterFields = true,
            RecordboundTransformer = TransformRecordToSayHello,
            OutputDataKind = KindOfTextData.Raw,
            OutputFileName = "output.txt"
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

      private static IRecord TransformRecordToSayHello(IRecord recordIn)
      {
         var recOut = recordIn.GetEmptyClone();
         recOut.AddItem("MSG", $"{recordIn["FNAME"]} {recordIn["LNAME"]} says Hello from {recordIn["CITY"]}!");
         return recOut;
         //Alternative syntax using dynamic type:
         //dynamic recOut = recordIn.GetEmptyClone();
         //dynamic recIn = recordIn;
         //recOut.MSG = $"{recIn.FNAME} {recIn.LNAME} says Howdy from {recIn.CITY}!";
         //return recOut;
      }
   }
}
