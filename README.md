# samples-gridgain-reliableevents

Demonstrates how to reliably handle cache events in Apache Ignite or GridGain.

## Setup

Prerequisites:
- IGNITE_HOME environment variable points to where GridGain CE 8.7.6.
- .NET Framework 4.x

## How To Use

- Run two Ignite server nodes. In two CMD terminals run:

  `cd Samples.GridGain.ReliableEvents\bin\Debug`

  `%IGNITE_HOME%\platforms\dotnet\bin\Apache.Ignite.exe -ConfigFileName=Samples.GridGain.ReliableEvents.exe.config -assembly=Samples.GridGain.ReliableEvents.exe -J-DIGNITE_QUIET=false`
  
- Run Samples.GridGain.ReliableEvents.exe

- Kill the server node executing the EventHandlerService. That node should output entries like `>>> EventHandlerService is executing` to the console.

- See the other node picked the next entry next to the one the failed node handled.

