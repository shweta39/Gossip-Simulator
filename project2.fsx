#time "on"
#r "nuget:Akka.Fsharp"
#r "nuget:Akka.Testkit"

open System
open System.Diagnostics
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit
open System.Text
open System.Numerics
open System.Collections.Generic

let system = System.create "system" (Configuration.defaultConfig())  //create the system 
//Storing the user input in variables
let inputNodes = int fsi.CommandLineArgs.[1] //Store the number of nodes in input nodes 
let inputinputTopology = fsi.CommandLineArgs.[2]  //The topology selected by user
let inputAlgo = fsi.CommandLineArgs.[3]  // The algorithm selected by user 
    
let start = DateTime.Now
let timer = Diagnostics.Stopwatch.StartNew()

type MessageReceive =
    | StartGossipParent of int * string * string
    | GossipNode of int * string
    | EndGossip of string * int
    | DoneGossipParent of string
    | PushSum of int * float * float
    | EndPushSum of string * int * float * float
    | EndPushSumParent of string * int * float

type SampleTuple = float * float

let rand = Random(1234)
let mutable randIndex = rand.Next()
let neighbourMap = new Dictionary<int, List<int>>()  //creating a dictionary to store neighbour list for each node
let nodeRatio = new List<float>()
let mutable endFlag = false 

for i in 0 .. inputNodes - 1 do
    nodeRatio.Add(float 0)


let FullGrid (inputNodes: int) =  //arrange the nodes in Full Network topology 
                    for i = 0 to inputNodes - 1 do
                        let gridList = new List<int>()
                        for j = 0 to inputNodes - 1 do
                            if (i <> j) then gridList.Add(j)
                        neighbourMap.Add(i, gridList)
               

let LineGrid (inputNodes: int) =  //arrange the nodes in Line topology 
                    for i = 0 to inputNodes - 1 do
                        if (i = 0) then // for the first node
                            let gridList = new List<int>() // creating a list of neighbour for ith node
                            gridList.Add(i + 1)
                            neighbourMap.Add(i, gridList) 
                        elif (i = inputNodes - 1) then // for the last node
                            let gridList = new List<int>()
                            gridList.Add(i - 1)
                            neighbourMap.Add(i, gridList)
                        else
                            let gridList = new List<int>()
                            gridList.Add(i - 1)
                            gridList.Add(i + 1)
                            neighbourMap.Add(i, gridList)


let Grid3D(inputNodes: int) =  //arrange the nodes in 3D topology 
                    let side: int = int (pown inputNodes 1/3) //calculate side of a cube
                    for i = 0 to inputNodes - 1 do

                        let gridList = new List<int>() // creating a new list
                        
                        if ((i - 1) >= 0) then gridList.Add(i - 1)
                        if ((i - side) >= 0) then gridList.Add(i - side)
                        if ((i - pown side 2) >= 0) then gridList.Add(i - pown side 2)
                        if ((i + 1) < inputNodes) then gridList.Add(i + 1)
                        if ((i + side) < inputNodes) then gridList.Add(i + side)
                        if ((i + pown side 2) < inputNodes) then gridList.Add(i + pown side 2)
                        
                        neighbourMap.Add(i, gridList) // add it as a neighbour to this node


let Imperfect3DGrid (inputNodes: int) = //arrange the nodes in Imperfect 3D topology 
                    let side: int = int (pown inputNodes 1/3)
                    for i = 0 to inputNodes - 1 do

                        let gridList = new List<int>() // creating a new list
                        
                        if ((i - 1) >= 0) then gridList.Add(i - 1)
                        if ((i - side) >= 0) then gridList.Add(i - side)
                        if ((i - pown side 2) >= 0) then gridList.Add(i - pown side 2)
                        if ((i + 1) < inputNodes) then gridList.Add(i + 1)
                        if ((i + side) < inputNodes) then gridList.Add(i + side)
                        if ((i + pown side 2) < inputNodes) then gridList.Add(i + pown side 2)


                        let mutable randIndex = rand.Next(inputNodes - 1) //pick a random node from the list 

                        while gridList.Contains(randIndex) && randIndex <> i do
                            randIndex <- rand.Next(inputNodes - 1)
                        neighbourMap.Add(i, gridList) // add it as a neighbour to this node



let square x=
    x=x*x;                  

let childNode id (mailbox: Actor<_>) =  //defination of child function
    
    let rec nodeLoop (nodeCount: int)
                     (pushsumFlag: bool)
                     (nodeSum: float)
                     (nodeWeight: float)
                     (endCounter: int)
                     (prevRatio: float)
                     ()
                     =
        actor {
            let! messageReceive = mailbox.Receive() 
            let mutable tempSum = nodeSum
            let mutable tempWeight = nodeWeight
            let mutable tempFlag = pushsumFlag
            let mutable tempCount = endCounter

            let mutable updatedPrevRatio = prevRatio 
            let x= square(id)
            match messageReceive with
            

            | PushSum (index, sum, weight) -> // add sum to current sum and do half og it and pass to another node
                if tempFlag then
                    tempSum <- tempSum + sum 
                    tempWeight <- tempWeight + weight
                    let ratio = tempSum / tempWeight
                    tempSum <- tempSum / float 2
                    tempWeight <- tempWeight / float 2

                    if Math.Abs(updatedPrevRatio - ratio)
                       <= float 0.0000000001 then
                        if tempCount = 2 then
                            tempFlag <- false
                            printfn "Node finished: %d\n" index 
                            system.ActorSelection("user/GossipParent")
                            <! EndPushSumParent("Done", index, ratio) //send message to the end push sum parent function
                        else
                            tempCount <- tempCount + 1
                    else
                        tempCount <- 0
                    updatedPrevRatio <- ratio

                    let len = neighbourMap.Item(index).Count
                    let randIndex = rand.Next(len)
                    let neighbourIndex = neighbourMap.Item(index).Item(randIndex)


                    system.ActorSelection("user/Node" + string neighbourIndex)
                    <! PushSum(neighbourIndex, tempSum, tempWeight) //send message to the Push sum function
                    system.ActorSelection("user/Node" + string index)
                    <! PushSum(index, tempSum, tempWeight) //send message to the Push sum function
                else
                    system.ActorSelection(String.Concat("user/Node", index))
                    <! EndPushSum("Done", index, sum, weight) //send message to Push sum function

            | EndPushSum (someString, index1, sum1, weight1) ->  
                let len = neighbourMap.Item(index1).Count
                randIndex <- rand.Next(len) //generate a random value 
                let neighbour = neighbourMap.Item(index1).Item(randIndex)
                system.ActorSelection(String.Concat("user/Node", neighbour))
                <! PushSum(neighbour, sum1, weight1) //send message to Push sum function
              
            |GossipNode (index, message) ->
                if nodeCount < 10 then 
                    if nodeCount = 9 then 
                        printfn "Finished Node: %d \n" id // this node is finished 
                        system.ActorSelection("user/GossipParent")
                        <! DoneGossipParent("Done") //send message to node

                    let len = neighbourMap.Item(index).Count
                    let randIndex = rand.Next(len) // Get the a random number
                    let neighbourIndex = neighbourMap.Item(index).Item(randIndex) // Get the exact index of the node based on list index

                    system.ActorSelection(String.Concat("user/Node", neighbourIndex))
                    <! GossipNode(neighbourIndex, message) //send message to the gossip node
                    system.ActorSelection(String.Concat("user/Node", index))
                    <! GossipNode(index, message) //send message to the gossip node
                else //the node has completed its work and transmitted the message to 10 nodes
                    system.ActorSelection(String.Concat("user/Node", index))
                    <! EndGossip("Done", index) //send message to the end gossip function

            | EndGossip (someString, index) -> 
                let len = neighbourMap.Item(index).Count 
                let randIndex = rand.Next(len)
                let neighbourIndex = neighbourMap.Item(index).Item(randIndex)
                system.ActorSelection(String.Concat("user/Node", neighbourIndex))
                <! GossipNode(neighbourIndex, "Gossip") //send message to the end gossip function

            | _ -> failwith "node failiure" // if invalid function call

            return! nodeLoop (nodeCount + 1) tempFlag tempSum tempWeight tempCount updatedPrevRatio () //calling the function again 
        }

    nodeLoop 0 true (float id) (float 1) 0 (float 1000) () //calling recurrsive function

let matchalgo inputAlgo =
        let mutable y = 0
        if inputAlgo="Gossip" then
            y<-0
        if inputAlgo="Push Sum" then
            y<-1
        y


let Parent =    // Create Parent actor
    spawn system "GossipParent"
    <| fun mailbox ->
        let rec parentLoop parentCount () =
            actor {
                
                let! messageReceive = mailbox.Receive()  // store the received message 

                match messageReceive with
                | StartGossipParent (inputNodes, inputTopology, inputAlgo) ->
                    printfn "\nTOPOLOGY = %s \nNUMBER OF NODES = %i \nALGORITHM = %s" inputTopology inputNodes inputAlgo 
                    match inputTopology with  //calling topology functions
                    | "Line" -> LineGrid inputNodes
                    | "3D" -> Grid3D inputNodes
                    | "FullNetwork" -> FullGrid inputNodes
                    | "Imperfect3D" -> Imperfect3DGrid inputNodes
                    | _ -> failwith "Invalid Topology entered"

                    let allNodes =  //create the nodes equal to the input nodes amount 
                        [ 0 .. inputNodes - 1 ]
                        |> List.map (fun id -> spawn system ("Node" + string id) (childNode id))

                    randIndex <- rand.Next(inputNodes - 1)
                    
                    let (z:int)= matchalgo(inputAlgo)
                    if z=0 then
                        allNodes.Item(randIndex) 
                        <! GossipNode(randIndex, "Hello")  //send message to Gossip node and start the gossip 
                    if z=1 then 
                        allNodes.Item(randIndex)
                        <! PushSum(randIndex, float 0, float 1) //send message to Push Sum algo and start the algorithm



                | DoneGossipParent (someMsg) -> //end the convergence for Gossip algorithm
                    
                    if parentCount = inputNodes - 1 then 
                        printfn "\nAll nodes are converged"
                        endFlag <- true


                | EndPushSumParent ("Done", index, ratio) -> //End Push sum algorithm 
                    nodeRatio.Item(index) <- ratio
                    
                    if parentCount = inputNodes-1 then //if counter reached the end of node
                        printfn "\nAll nodes are converged"
                        endFlag <- true

                | _ -> failwith "Error occured"

                return! parentLoop (parentCount + 1) () //rescurssion 

            }

        parentLoop 0 ()


let main(args) = 
       
    Parent  
    <! StartGossipParent(inputNodes, inputinputTopology, inputAlgo) //Send this message to the Master with the iputs given by User

    while not endFlag do // end the program if end flag turns true otherwise ignore 
        ignore ()
    //Code to compute Time
    timer.Stop()
    printfn"\nTotal time taken to converge = %f ms\n" timer.Elapsed.TotalMilliseconds
    0

main()  // calling the main function

