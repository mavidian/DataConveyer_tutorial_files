using Mavidian.DataConveyer;
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
            //Not available in free edition of Data Conveyer:
            //LoggerType = Mavidian.DataConveyer.Logging.LoggerType.LogFile,
            //LoggingThreshold = Mavidian.DataConveyer.Logging.LogEntrySeverity.Information,
            IntakeRecordLimit = 180,
            TimeLimit = TimeSpan.FromSeconds(1.5),
            ReportProgress = true,
            ProgressInterval = 1,
            ProgressChangedHandler = (s, e) => { if (e.Phase == Phase.Output) Console.Write($"\rProcessed {e.RecCnt.ToString()} records so far..."); },
            InputDataKind = KindOfTextData.CSV,
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

            //Our execution task is running, let's start another task that will cancel it when a key is pressed before execution task completes
            Task.Run(() => { while (!Console.KeyAvailable) { } Console.ReadKey(true); orchtr.CancelExecution(); });

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
         if (recordIn.RecNo > 42) throw new Exception("Simulated problem occurred");
         return recOut;
      }
   }
}