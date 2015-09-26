#r "EventStore.ClientAPI.dll"
#r "EventStore.ClientAPI.Embedded.dll"
#r "Newtonsoft.Json.dll"

open System
open System.Threading
open System.Text
open System.Threading.Tasks

open Newtonsoft.Json

open EventStore.Core
open EventStore.Core.Messages
open EventStore.Core.Bus
open EventStore.ClientAPI
open EventStore.ClientAPI.Embedded
open EventStore.ClientAPI.Common.Log

let fromJson<'a> byteArray=
    let json = System.Text.Encoding.UTF8.GetString(byteArray)
    JsonConvert.DeserializeObject<'a>(json) //'

let toJson<'a> (obj:'a) =
    JsonConvert.SerializeObject(obj)
    |> System.Text.Encoding.UTF8.GetBytes

let awaitTask (t:Task) = t.ContinueWith( fun t -> ()) |> Async.AwaitTask

module Async =
  let AwaitVoidTask (task : Task) : Async<unit> =
      Async.FromContinuations(fun (cont, econt, ccont) ->
          task.ContinueWith(fun task ->
              if task.IsFaulted then econt task.Exception
              elif task.IsCanceled then ccont (OperationCanceledException())
              else cont ()) |> ignore)



/// Execute f in a context where you can bind to the event store
let withEmbeddedEs f =
    async {



      let node = EmbeddedVNodeBuilder.AsSingleNode()
                                   .OnDefaultEndpoints()
                                   .WithExternalTcpOn(new Net.IPEndPoint(Net.IPAddress.Parse("127.0.0.1"),1789))
                                   .WithInternalTcpOn(new Net.IPEndPoint(Net.IPAddress.Parse("127.0.0.1"),1790))
                                   .WithExternalHttpOn(new Net.IPEndPoint(Net.IPAddress.Parse("127.0.0.1"),1790))
                                   .WithInternalHttpOn(new Net.IPEndPoint(Net.IPAddress.Parse("127.0.0.1"),1790))
                                   .RunInMemory()
                                   .RunProjections(ProjectionsMode.All)
                                   .WithWorkerThreads(16)
                                   .Build()

      printfn "starting embedded EventStore"
      node.Start()
      let! result = f()

      printfn "stopping embedded EventStore"
      let stopped = new AutoResetEvent(false)
      node.MainBus.Subscribe( new AdHocHandler<SystemMessage.BecomeShutdown>(fun m -> stopped.Set() |> ignore))
      node.Stop() |> ignore
      if not (stopped.WaitOne(20000)) then
        return "couldn't stop ES within 20000 ms"
      else
        return "stopped embedded EventStore"
    }

type Enveloppe = {
  version : int
}

let handleAsync<'T> apply streamName conn (i, v, cmd) =

  let saveEvtsAsync (conn:IEventStoreConnection) stream (id, v, evts) = async {
    let typeEvts = (typedefof<'T>).ToString() //'
    let enveloppeMetadata = toJson<Enveloppe>({ version= v })

    let events = evts
                  |> List.mapi (fun index e ->
                                let data=  toJson<'T> e //'
                                let enveloppeMetadata = toJson<Enveloppe>({ version= v })
                                new EventData(id,typeEvts,true, data,enveloppeMetadata)
                                )


    let expectedVersion = v
    let n = sprintf "%s - %s" streamName (i.ToString())


    printfn "uncommitted events have been produced according to version %i" expectedVersion
    let! writeResult = conn.AppendToStreamAsync(n,expectedVersion,events) |> Async.AwaitTask

    printfn "events appended to %s, next expected version : %i"  n writeResult.NextExpectedVersion

  }

  async {
    printfn "processing some complex stuff"
    let evts = apply cmd
    printfn "save uncommitted events"
    return! saveEvtsAsync conn streamName (i, v, evts)
  }

let hydrateAggAsync<'T>  streamName applyTo initialState (conn:IEventStoreConnection) id =

  let rec applyRemainingEvents evts =
      match evts with
      | [] -> initialState
      | head :: tail ->  applyTo (applyRemainingEvents tail) head


  async {

    printfn "reading stream events..."
    let n = sprintf "%s - %s" streamName (id.ToString())
    let! slice = conn.ReadStreamEventsForwardAsync(n,0,99,false) |> Async.AwaitTask

    let evts =
      slice.Events
      |> Seq.map (fun (e:ResolvedEvent) -> fromJson<'T> e.Event.Data) //'
      |> Seq.toList
    printfn "%i events read" evts.Length
    evts |> List.iteri (fun i e-> printfn "applying event %i" i)
    let aggregate = evts |> applyRemainingEvents

    return aggregate
  }



// **************************
// FakeDomain
// **************************

module MyDomain =

  let streamName= "MyDomainAgrgegate"

  type State = {
    count1 : int;
    count2 : int;
  }
  with static member Initial = { count1 =0; count2=0; }

  type Cmd =
    | Cmd1
    | Cmd2

  type Evt =
    | Evt1
    | Evt2
    | Evt3

  let applyTo = function
    | Cmd1 -> [Evt1]
    | Cmd2 -> [Evt2;Evt3]

  let applyEvt state = function
  | Evt1 -> { state with count1=state.count1 + 3}
  | Evt2 -> { state with count2=state.count2 + 1}
  | _ -> state

let sampleAppAsync =
  fun () -> async {

    //connect to ges
    let connectionSettings =  EventStore.ClientAPI.ConnectionString.GetConnectionSettings("ConnectTo=tcp://admin:changeit@127.0.0.1:1789")
    let conn = EventStore.ClientAPI.EventStoreConnection.Create(connectionSettings,new Uri("tcp://admin:changeit@127.0.0.1:1789"),"maConnection")

    conn.ConnectAsync() |>  awaitTask |> Async.RunSynchronously


    //create the handler for this aggegate
    let handleMyDomainCmdAsync =  handleAsync<MyDomain.Evt> MyDomain.applyTo MyDomain.streamName conn


    let idAggregate = Guid.NewGuid()

    printfn " "
    printfn "------------------------------------------------------"
    printfn "handle fake command Cmd2 which will produce  2 events"
    do! handleMyDomainCmdAsync  (idAggregate , ExpectedVersion.NoStream, MyDomain.Cmd2)

    printfn " "
    printfn "------------------------------------------------------"
    printfn "handle fake command Cmd1 which will produce  1 events"
    do! handleMyDomainCmdAsync  (idAggregate , 1, MyDomain.Cmd1)

    printfn " "
    printfn "------------------------------------------------------"
    printfn "hydrating fake Agg from events produced by previous command"
    let! myDomainAggState =  hydrateAggAsync<MyDomain.Evt> MyDomain.streamName MyDomain.applyEvt MyDomain.State.Initial conn idAggregate

    printfn " "
    printfn "------------------------------------------------------"
    printfn "Current state of aggregate now holds"
    printfn " - count1 : %i" myDomainAggState.count1
    printfn " - count2 : %i" myDomainAggState.count2

    conn.Close()
  }

let result = withEmbeddedEs sampleAppAsync |> Async.RunSynchronously
