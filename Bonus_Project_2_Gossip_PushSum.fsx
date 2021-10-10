
#r "nuget: Akka.FSharp"

open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration
open System.Collections.Generic


let system = System.create "Gossip" (Configuration.defaultConfig());

let mutable numNodes = (int)fsi.CommandLineArgs.[1] 
let temp = numNodes |> float
let mutable toKill = (int) fsi.CommandLineArgs.[4]
let mutable deadList :int [] = [||]
let topology = fsi.CommandLineArgs.[2].ToLower()
let protocol = fsi.CommandLineArgs.[3].ToLower()
let timer = System.Diagnostics.Stopwatch()
let actorPositionTable = new Dictionary<IActorRef,int>()
// Create and initailise the array to store neighbours of an actor
let mutable actorNeighbours = [||]


// A dictionary to store if the actor is live or has converged
let actorTable = new Dictionary<IActorRef, bool>()

//Pushsum parameters
let mutable w = 1.0
let terminationThreshold = (10.00 ** -10.00)

type Messages = 
    | ActorInitialize of IActorRef []
    | VariableIntitialize of int
    | StartChatting of String
    | ReportMessageReceived of String
    | BeginPushSum
    | Start of String
    | CalculatePushSum of Double * Double * Double * bool
    | Answer of Double * Double
    | Time of int
    | Converged of float
    | StartGossip
    | TotalNodes of int
    | StartWorker 
    | AssignNeighbour
    | TransmitRumor
    | FinishedRumor
    | Begin
    | SetNeighbours of IActorRef []
    | ReSpawnTerminatedNeighbours
    | ReSpawnTerminatedNeighbours2
    | INIT_PUSHUM of float
    | ReportTermination
    | SetDeadNeighbours of int[]
    | StartPushSum

type TopologyType = 
    | Gossip of String
    | PushSum of String

type ProtocolType = 
    | Line of String
    | Full of String
    | TwoDimension of String

// Monitoring Unit of the system (Takes command and reports to boss actor)
let mutable supervisorRefGlobal: IActorRef =null
let mutable bossRefGlobal: IActorRef =null

let SpawningActor (mailbox: Actor<_>) =
    let neighbors = new List<IActorRef>()
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        | ReSpawnTerminatedNeighbours _ ->
            for i in [0..numNodes-1] do
                    neighbors.Add actorNeighbours.[i]
            mailbox.Self <! StartGossip
          
        | StartGossip ->
            if neighbors.Count > 0 then
                let randomNumber = Random().Next(neighbors.Count)
                let randomActor = neighbors.[randomNumber]
                
                if (actorTable.[neighbors.[randomNumber]]) then  
                    (neighbors.Remove randomActor) |>ignore
                else 
                    randomActor <! TransmitRumor
                mailbox.Self <! StartGossip 
        | StartPushSum ->
            if neighbors.Count > 0 then
                let randomNumber = Random().Next(neighbors.Count)
                let randomActor = neighbors.[randomNumber]
                
                if (actorTable.[neighbors.[randomNumber]]) then  
                    (neighbors.Remove randomActor) |>ignore
                else 
                    randomActor <! BeginPushSum
                mailbox.Self <! StartPushSum 
        
        | ReSpawnTerminatedNeighbours2 _ ->
            for i in [0..numNodes-1] do
                    neighbors.Add actorNeighbours.[i]
            mailbox.Self <! StartPushSum

        | _ -> ()
        return! loop()
    }
    loop()

let ReSpawningActorRef = spawn system "SpawningActor" SpawningActor

let worker(mailbox: Actor<_>)=
    
    // Gossip parameters
    let mutable neighbours:IActorRef[] = [||]
    let mutable rumourHeard = 0;

   
    // Pushum parameters
    let mutable s=0.0;
    let mutable terminated = false;
    let mutable consecutiveRounds = 0;
    let mutable sumEstimationRatioDiff = 0.0

    // DeadZone parameters
    let mutable deadArray :int[] = [||]
    let mutable defective = false

    let rec loop()=actor{
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

     
        

        match msg with

        // ------------------  MESSAGE PATTERN FAILURE NDOES  --------------------
    
        |SetDeadNeighbours l ->
            deadArray <-l
            if Array.contains actorPositionTable.[mailbox.Self] deadArray then
                defective <-true
    
    
    
        // ------------------  MESSAGE PATTERN FOR GOSSIP  --------------------

        // Setting the neighbours list to the values returned by the supervisor
        | SetNeighbours nbs ->
            neighbours <-nbs
            

        | StartGossip ->  
            // printf "Starting first time "
            if rumourHeard <=10 then 
                let nextActorIndex = Random().Next(0,neighbours.Length)
                // printf "next index %d " nextActorIndex
                if not actorTable.[neighbours.[nextActorIndex]] then //Check if the neighbour is still live
                    neighbours.[nextActorIndex]<!TransmitRumor //Send the rumour to the random neighbour
                mailbox.Self <!StartGossip

        | TransmitRumor -> 
            
            if rumourHeard = 0 then // if this is the first time for the actor
                mailbox.Self <! StartGossip //Forward to next random actor
            
            if (rumourHeard = 10) then // if finsihed hearing 10 times
                supervisorRefGlobal <! FinishedRumor //Report to the supervisor
                actorTable.[mailbox.Self] <- true // Set your finsihed status to true
            rumourHeard <- rumourHeard + 1 //Increment its rumour
        




        // ------------------  MESSAGE PATTERN FOR GOSSIP  --------------------


        | INIT_PUSHUM value ->  
            s <- value

        | BeginPushSum -> 
            
            s <- s / 2.0
            w <- w / 2.0

            let nextRandomActor = Random().Next(0,neighbours.Length)

            neighbours.[nextRandomActor] <! CalculatePushSum (s,w,terminationThreshold,true)

        
        | CalculatePushSum (s1,w1,terminationThreshold,flag) ->
            
            // Ignore PUSHSUM for defective actor
            if(not defective || flag) then   
                
                // If the calling actor is dead, make NO CHANGEs to variables and call new actor
                if terminated then
                    let idx = Random().Next(0,neighbours.Length)
                    neighbours.[idx] <! CalculatePushSum (s,w,terminationThreshold,false)

                // Update values of pushsum and check convergence
                else

                    // RECEIVE FUNCTIONALITY
                    
                    // Update the values for the current actor
                    let newS = s + s1
                    let newW = w + w1

                    // Calculate ratio difference from previous call
                    sumEstimationRatioDiff <- abs((s/w) - (newS/newW))

                    if sumEstimationRatioDiff <= terminationThreshold then

                        consecutiveRounds <- consecutiveRounds + 1

                    elif sumEstimationRatioDiff > terminationThreshold then // Reset the counter if the contiuity breaks

                        consecutiveRounds <- 0

                    if consecutiveRounds = 3 then // Terminate the actor and report to supervisor
                        
                        supervisorRefGlobal<!ReportTermination
                        terminated <- true
                
                    // SEND FUNCTIONALITY

                    s <- newS / 2.0
                    w <- newW / 2.0

                    let idx = Random().Next(0,neighbours.Length)
                    neighbours.[idx] <! CalculatePushSum (s,w,terminationThreshold,false)
                

        |(_) -> ()

        return! loop()

    }
    loop()

let workerRef = spawn system "worker" worker

let createActors(n:int) =
    printfn "Creating %d actors....." numNodes
    actorNeighbours<-Array.zeroCreate(n+1)
    for i in [0..n] do
        let name:string="Worker" + string(i)
        let actorRef = spawn system (name) worker
        actorNeighbours.[i] <- actorRef 
        actorTable.Add(actorNeighbours.[i], false)
        actorPositionTable.Add(actorNeighbours.[i], i)

        // INITIALISING PUSH SUM Actor variables
        actorNeighbours.[i] <! INIT_PUSHUM ((float)i) //for pushsum
    printfn ("Done - Actors created")
    actorNeighbours

// Create and set neighbours for LINE topology    
let setLineNeighbours (actors:IActorRef[])=
    printfn "Setting neighbours....." 
    for x in [0 .. numNodes] do
        let mutable nblist = [||]
        if x=0 then
            nblist <- (Array.append nblist [|actors.[x+1]|])
        elif x=numNodes then
            nblist <- (Array.append nblist [|actors.[x-1]|])
        else
            nblist <- (Array.append nblist [|   actors.[(x+1)]; actors.[(x-1)]    |])

        actors.[x] <!SetNeighbours(nblist)
    printfn("Done - All Neighbours set")

// Create and set neighbours for FULL topology    
let setFullNeighbours (actors:IActorRef[])=
    printfn "Setting %d neighbours (this may take a while)....." numNodes
    for x in [0..numNodes] do
        let mutable nblist = [||]
        if (x % (numNodes/5)) = 0 then printfn "Loading...(%d/%d) neighbours set" x numNodes
        for y in [0..numNodes] do
            if x <> y then
                nblist <- Array.append nblist [|actors.[y]|]
        actors.[x]<!SetNeighbours(nblist)

// Create and set neighbours for 3D GRID topology    
let set3DNeighbours(actors:IActorRef[],n:int,side:int)=
    printfn "Setting %d neighbours to fit in the 3d grid (this may take a while)..." n
    
    let mutable nblist = [||]
    let mutable c =0;
    for x in [0..n] do
        if (x % (n/5)) = 0 then printfn "Loading...(%d/%d) neighbours set" x numNodes
        
        if (x-1>=0)  then nblist<- (Array.append nblist [|actors.[x-1]|])
        if (x+1<n)  then nblist<- (Array.append nblist [|actors.[x+1]|])
        if (x-side>=0) then nblist<- (Array.append nblist [|actors.[x-side]|])
        if (x+side<n)  then nblist<- (Array.append nblist [|actors.[x+side]|])
        if (x-(side*side)>=0) then nblist<- (Array.append nblist [|actors.[x-(side*side)]|])
        if (x+(side*side)<n)  then nblist<- (Array.append nblist [|actors.[x+(side*side)]|])
       
        actors.[x]<!SetNeighbours(nblist)
        
    printfn("Done - All Neighbours set")
     // Setting neighbours
    
let setImp3DNeighbours(actors:IActorRef[],n:int,side:int)=
    printfn "Setting %d neighbours to fit in the 3d grid (this may take a while)..." n
    
    let mutable nblist = [||]
    let mutable c =0;
    for x in [0..n] do
        if (x % (n/5)) = 0 then printfn "Loading...(%d/%d) neighbours set" x numNodes

        if (x-1>=0)  then nblist<- (Array.append nblist [|actors.[x-1]|])
        if (x+1<n)  then nblist<- (Array.append nblist [|actors.[x+1]|])
        if (x-side>=0) then nblist<- (Array.append nblist [|actors.[x-side]|])
        if (x+side<n)  then nblist<- (Array.append nblist [|actors.[x+side]|])
        if (x-(side*side)>=0) then nblist<- (Array.append nblist [|actors.[x-(side*side)]|])
        if (x+(side*side)<n)  then nblist<- (Array.append nblist [|actors.[x+(side*side)]|])
       
        // Adding a new random actor
        let newRandomActor = Random().Next(0,n-1);
        nblist <- Array.append nblist [|actors.[newRandomActor]|]
        actors.[x]<!SetNeighbours(nblist)
        
    printfn("Done - All Neighbours set")
     // Setting neighbours

let killActors(actors:IActorRef[])=
    printfn("Creating deadMan List....")
    System.Threading.Thread.Sleep(3000);

    for i = 1 to toKill do
        let mutable deadManNumber = Random().Next(0,numNodes)
        deadList <- Array.append deadList [|deadManNumber|]
        printfn("Actor %d dead!") deadManNumber

    // Update the index table for the dead actors
    for i=1 to numNodes do
        actors.[i] <! SetDeadNeighbours (deadList)

    printfn("Killing complete")
    System.Threading.Thread.Sleep(3000);


let supervisor (mailbox:Actor<_>) = 
    
    let mutable finishedWorkers = 0;
    
    let rec loop() = actor{
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()

        


        match msg with
        | Begin -> 
            
            printfn "\t-----------------------------------------------------------------------"    
            printfn "\tStarting %s Protocol with %s Topology and %d failure nodes....." protocol topology toKill
            printfn "\t-----------------------------------------------------------------------"
            System.Threading.Thread.Sleep(5000);
           
            

            match topology with

            |"line"->
                
                // Create n actors
                let actors:IActorRef[] = createActors(numNodes)
                
                // Kill actors
                killActors(actors)
                
                // Create and set neighbours
                setLineNeighbours(actors)

                // Setting the actor with begining message
                let leaderActor = Random().Next(0,numNodes)

                // Starting timer
                timer.Start()
                match protocol with
                | "gossip" -> 
                    printfn("\n\tBegining conversation")
                    actors.[leaderActor]<!StartGossip
                    ReSpawningActorRef<! ReSpawnTerminatedNeighbours
 
                | "push-sum" ->       
                    printfn("\n\tBegining conversation")
                    actors.[leaderActor]<!BeginPushSum
                    ReSpawningActorRef<! ReSpawnTerminatedNeighbours2

                |(_)-> printfn "Incorrect protocol"
 
            |"full" -> 
                // Create n actors
                let actors:IActorRef[] = createActors(numNodes)

                // Kill actors
                killActors(actors)
                
                // Create and set neighbours
                setFullNeighbours(actors)

                // Setting the actor with begining message
                let leaderActor = Random().Next(0,numNodes)

                // Starting timer
                timer.Start()
                match protocol with
                | "gossip" -> 
                    
                    printfn("\n\tBegining conversation")
                    actors.[leaderActor]<!TransmitRumor

                | "push-sum" -> 
                    printfn("\n\tBegining conversation")      
                    actors.[leaderActor]<!BeginPushSum
                    ReSpawningActorRef<! ReSpawnTerminatedNeighbours
                    ReSpawningActorRef<! ReSpawnTerminatedNeighbours2

                |(_)-> printfn "Incorrect protocol"



            |"3d"->
                
                //Total Actor Calculation
                let side = int(round((float numNodes)**(1.0/3.0)))

                let totalWorkers = (int)((float side**3.0)|>ceil)
                numNodes<-totalWorkers

                // Create n actors
                let actors:IActorRef[] = createActors(totalWorkers)

                // Kill actors
                killActors(actors)
                
                // Create and set neighbours
                set3DNeighbours(actors,totalWorkers,side)

                // Setting the actor with begining message
                let leaderActor = Random().Next(0,numNodes)

                // Starting timer
                timer.Start()
                match protocol with
                | "gossip" -> 
                    printfn("\n\tBegining conversation")
                    actors.[leaderActor]<!StartGossip
                    ReSpawningActorRef<! ReSpawnTerminatedNeighbours
 
                | "push-sum" -> 
                    printfn("\n\tBegining conversation")      
                    actors.[leaderActor]<!BeginPushSum
                    ReSpawningActorRef<! ReSpawnTerminatedNeighbours
                    ReSpawningActorRef<! ReSpawnTerminatedNeighbours2

                |(_)-> printfn "Incorrect protocol"

            |"imp3d"->
                
                //Total Actor Calculation
                let side = int(round((float numNodes)**(1.0/3.0)))

                let totalWorkers = (int)((float side**3.0)|>ceil)
                numNodes<-totalWorkers

                // Create n actors
                let actors:IActorRef[] = createActors(totalWorkers)

                // Kill actors
                killActors(actors)
                
                // Create and set neighbours
                setImp3DNeighbours(actors,totalWorkers,side)

                // Setting the actor with begining message
                let leaderActor = Random().Next(0,numNodes)

                // Starting timer
                timer.Start()
                match protocol with
                | "gossip" -> 
                    printfn("\n\tBegining conversation")
                    actors.[leaderActor]<!StartGossip
                    ReSpawningActorRef<! ReSpawnTerminatedNeighbours
 
                | "push-sum" -> 
                    printfn("\n\tBegining conversation")       
                    actors.[leaderActor]<!BeginPushSum
                    ReSpawningActorRef<! ReSpawnTerminatedNeighbours
                    ReSpawningActorRef<! ReSpawnTerminatedNeighbours2

                |(_)-> printfn "Incorrect protocol"
            
            |(_) -> printfn "Incorrect topology"    

        |FinishedRumor ->
            finishedWorkers <- finishedWorkers+1

            // Progress Tracking
            if (finishedWorkers<=numNodes) then
                printfn "Total workers finished %d" finishedWorkers

            // Update the boss that all workers have converged
            if(finishedWorkers = numNodes-toKill) then 
                timer.Stop()
                let endingTime = timer.Elapsed.TotalMilliseconds
                bossRefGlobal<! Converged endingTime
                

        // --------------- MESSAGE MATCHING FOR PUSH SUM ------------

        | ReportTermination -> 

            finishedWorkers <- finishedWorkers+1

            // Progress Tracking
            if (finishedWorkers<=numNodes) then
                printfn "Total workers finished %d" finishedWorkers

            // Update the boss that all workers have converged
            if(finishedWorkers = numNodes-toKill) then 
                timer.Stop()
                let endingTime = timer.Elapsed.TotalMilliseconds
                bossRefGlobal<! Converged endingTime

        |(_) -> printfn "Incorrect message to supervisor"

        return! loop()

    }
    loop()

supervisorRefGlobal <- spawn system "supervisor" supervisor


// The main working horses of the algorithm



let boss (mailbox: Actor<_>)=
    
 
    let rec loop()= actor{
        let! msg = mailbox.Receive();
        
        
        match msg with 
        | Start s -> 
            if topology = "line" || topology = "full" || topology = "3d" || topology = "imp3d" then
                supervisorRefGlobal <!Begin
            else 
                printfn "Incorrect topology - terminating!!!\n"
                mailbox.Context.System.Terminate() |> ignore
  
        |Converged timeTaken ->
            System.Threading.Thread.Sleep(5000);
            printfn "\n\t------------------------------------------------"
            printfn "\t\tConverged with time %d ms" ((int)(round(timeTaken)))
            printfn "\t------------------------------------------------"
            
            mailbox.Context.System.Terminate() |> ignore
        


        | _ -> ()

        return! loop()
    }            
    loop()

bossRefGlobal<- spawn system "boss" boss

printfn "\n\n"

bossRefGlobal<!Start ("GoBabyGo")

system.WhenTerminated.Wait()

printfn "\n\n"

