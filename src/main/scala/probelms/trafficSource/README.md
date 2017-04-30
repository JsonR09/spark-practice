# Traffic Source

## Description

You are given a web log consisting of a list of events for each user.
Each event consists of userId, url visited and timestamp of activity.
Using this, output the traffic source distribution for a given url.

##### Input
###### WebLog: (UserId, URL, Timestamp)
eg
- 1 ABC T0
- 1 XYZ T2
- 1 PQR T1
- 2 DEF T2
- 3 PQR T3

###### TargetUrl: 
- PQR

##### Output
###### (TrafficSource, count) 
- (ABC,1)
- (DEF,1)
