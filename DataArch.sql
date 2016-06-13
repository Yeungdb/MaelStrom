create table Exp (
   EID serial primary key not null,
   ExpName text not null
);

create table ExpRecFile(
   EID integer references Exp(EID) not null,
   Filename text not null
);

create table ParameterListing(
   PID serial primary key not null,
   ParameterHash text not null
);

create table ParameterAttributes(
   APID serial primary key not null,
   ParameterName text not null
);

--COMMENT OUT THE BELOW INSERT STATEMENTS IF YOU HAVE DIFFERENT PARAMETER ASSIGNMENTS
insert into ParameterAttributes values (DEFAULT, 'ChromatogramType');
insert into ParameterAttributes values (DEFAULT, 'ScanIonMode');
insert into ParameterAttributes values (DEFAULT, 'VoltageListings');
insert into ParameterAttributes values (DEFAULT, 'MZLowerLimit');
insert into ParameterAttributes values (DEFAULT, 'MZUpperLimit');


create table RelateExpParameter(
   EID integer references Exp(EID) not null,
   PID integer references ParameterListing(PID) not null
);

create table ExParam(
   EID integer references Exp(EID) not null,
   PID integer references ParameterListing(PID) not null,
   APID integer references ParameterAttributes(APID) not null,
   ParamVal text not null --IE, SRM/SIM/TIC/FULL, POS/NEG ...
);

create table ExpComment(
   CID serial primary key not null,
   TextComment text not null
);

create table RelateExpComment(
   EID integer references Exp(EID) not null,
   CID integer references ExpComment(CID) not null
);

create table Channel(
   ChannelId serial primary key not null,
   ChannelName text not null
);

create table Time(
   EID integer references Exp(EID) not null,
   ChannelId integer references Channel(ChannelId) not null,
   Time real not null
);

create table Signal(
   EID integer references Exp(EID) not null,
   ChannelId integer references Channel(ChannelId) not null,
   Signal real not null
);

--create table TICMZ(
--   EID integer references Exp(EID) not null,
--   ChannelId integer references Channel(ChannelId) not null,
--   Time real not null,
--   MZ real not null
--);

--create table TICINT(
--   EID integer references Exp(EID) not null,
--   ChannelId integer references Channel(ChannelId) not null,
--   Time real not null,
--   Signal real not null
--);

create table TICCount(
   EID integer references Exp(EID) not null,
   ChannelId integer references Channel(ChannelId) not null,
   Time real not null,
   MZ real not null,
   Signal real not null
);

create table SIM(
   ChannelId integer references Channel(ChannelId) not null,
   SIMID serial primary key not null,
   MassQ1 real not null
);

create table SRM(
   ChannelId integer references Channel(ChannelId) not null,
   SRMID serial primary key not null, --MSMS
   MassQ1 real not null, --Parent
   MassQ3 real not null  --Progeny
);



