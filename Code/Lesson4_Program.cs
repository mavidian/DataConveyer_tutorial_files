using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Threading;
using System.Threading.Tasks;

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
            ReportProgress = true,
            ProgressInterval = 1,
            ProgressChangedHandler = (s, e) => { if (e.Phase == Phase.Output) Console.Write($"\rProcessed {e.RecCnt.ToString()} records so far..."); },
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
            var execTask = orchtr.ExecuteAsync();  //asynchronous execution

            //Our execution task is running, let's cancel it after 1 second
            Thread.Sleep(1000);
            orchtr.CancelExecution();

            //Our execution task is running, let's start another task that will cancel it when a key is pressed
            //Task.Run(async () => { while (!Console.KeyAvailable) await Task.Delay(50); Console.ReadKey(true); orchtr.CancelExecution(); });

            result = execTask.Result;
         }
         Console.WriteLine(" done!");

         //Evaluate completion status:
         switch (result.CompletionStatus)
         {
            case CompletionStatus.IntakeDepleted:
               Console.WriteLine($"Successfully processed {result.RowsWritten.ToString()} records");
               break;
            case CompletionStatus.InitializationError:
               Console.WriteLine("Oops! Initialization error - maybe the input file is missing(?)");
               break;
            case CompletionStatus.Canceled:
               Console.WriteLine($"Execution was canceled after processing {result.RowsWritten.ToString()} records");
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
         //var recOut = recordIn.GetEmptyClone();
         //recOut.AddItem("MSG", $"{recordIn["FNAME"]} {recordIn["LNAME"]} says Hello from {recordIn["CITY"]}!");
         //return recOut;
         //Alternative syntax using dynamic type:
         dynamic recOut = recordIn.GetEmptyClone();
         dynamic recIn = recordIn;
         recOut.MSG = $"{recIn.FNAME} {recIn.LNAME} says Howdy from {recIn.CITY}!";
         Thread.Sleep(15);
         return recOut;
      }
   }
}
