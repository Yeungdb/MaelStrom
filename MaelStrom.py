#!/usr/bin/python

import hashlib

file = open("DY-ESSI+ELSI-EDC-1.txt", "r")
Dictionary = {} #Testing for Duplicate Entries

#---------------------------
#Switches for File Iteration
ParseStart = 0
ChromatoParse = 0 #Don't parse until reached the Chromatogram sub-section
CVOrder = 0 #There are four cvParams with different meaning in a Chromatogram
            #Thus need to keep track on which one is being processed
            #Order is maintained for each Chromatogram set
            #1-Chromatogram Type (TIC, SRM ...)
            #2-Scan Type (Positive, Negative
            #3-TimeArray (Followed by the binary array of Time in Minutes)
            #4-IntensityArray (Followed by the binary array of Intensities in Counts)
#---------------------------

#---------------------------
#Defining Global Vars
FileId = 0
ChromatoId = 0
ChromatoType = 0 #TIC, SRM, MSMS for SplitParam[0]
ChromatoIonMode = 1 #Positive/Negativ for SplitParam[1]
TimeBinData = 3 #Binary Index in Array for Binary Data for Time 
IntensityBinData = 4 #Binary Indexin Array for Binary Data for Intensity
#---------------------------

numLines = 0
for linesOpt in file:
    numLines += 1

ParameterListing = ""

file.seek(0) #Refresh the File Cursor for reiteration

BinaryArray = []
for x in xrange(2):
    BinaryArray.append([])

ExperimentName = ""
ParameterListings = ""
ChromatoChannel = ""
for lines in file:
    Dictionary[lines]=hashlib.sha512(lines).hexdigest()
    leadSpace = len(lines)-len(lines.lstrip(' '))
    lines = lines.lstrip(' ')
    parsedData = lines.split(":")
    if(parsedData[0] == "run"):
      ParseStart = 1 

    if(ParseStart == 1 and (parsedData[0] == "id" and leadSpace == 4)): #Based on the File Hierarchy, 2->Run, 4->Id
      parsedData[1] = parsedData[1].lstrip(' ').strip('\n') 
      ExperimentName = parsedData[1]
      print """INSERT INTO Exp values (DEFAULT, "{ExpNAME}")""".format(ExpNAME=ExperimentName)
      print """INSERT INTO ExpRecFile values (DEFAULT, "{FileName}")""".format(FileName=ExperimentName+".raw")
      #Insert this into ExperimentName

    elif(ParseStart == 1 and (parsedData[0] == "chromatogram" and leadSpace == 6)): #Based on the File Hierarchy, 2->Run, 4->Id
      ChromatoParse = 1

    elif(ParseStart == 1 and ChromatoParse == 1 and (parsedData[0] == "id" and CVOrder == 0 and leadSpace >= 8)):
      #Chromatogram Identifier
      ChromatoChannel = parsedData[1].lstrip(' ').strip('\n')
      print """INSERT INTO Channel values (DEFAULT, "{ChannelName}")""".format(ChannelName = ChromatoChannel)
      ChromatoSplit = ChromatoChannel.split(" ")
      MassQuad = ""
      MassScanType = ""
      for ChannelInfo in ChromatoSplit:
         if("SIM" in ChannelInfo):
             MassScanType = "SIM"
         elif("SRM" in ChannelInfo):
             MassScanType = "SRM"
         elif("Q3" in ChannelInfo):
             MassQuad += ", " + ChannelInfo.split("=")[1]
         elif("Q1" in ChannelInfo):
             MassQuad += ChannelInfo.split("=")[1]
      if(ChromatoChannel != "TIC"):       
        print """INSERT INTO {MassScanType} values ((select ChannelId from Channel where ChannelName = "{ChannelName}"), DEFAULT, {QuadMass})""".format(MassScanType=MassScanType, ChannelName=ChromatoChannel, QuadMass = MassQuad)

    elif(ParseStart == 1 and ChromatoParse == 1 and (parsedData[0] == "cvParam" and CVOrder == 0 and leadSpace >= 8)): #NEED TO DIFFERENTIATE BETWEEN THE DIFFERRENT cvPARAM
      #Parse the Chromatogram type
      ParameterListings += parsedData[1].lstrip(' ').strip('\n')
      ParameterListings += ","
      CVOrder+=1

    elif(ParseStart == 1 and ChromatoParse == 1 and (parsedData[0] == "cvParam" and CVOrder == 1 and leadSpace >= 8)):   
      #Parse the Scan type  
      ParameterListings += parsedData[1].lstrip(' ').strip('\n')
      CVOrder+=1

    elif(ParseStart == 1 and ChromatoParse == 1 and (parsedData[0] == "binary" and CVOrder == 2 and leadSpace >= 8)):   
      #Set Parse Bin Time Array to True
      TimeBin = parsedData[1].split("] ")
      counter = 0
      for Time in TimeBin[1].strip('\n').strip(" ").split(" "):
         print """INSERT INTO Time values ((select EID from Exp where ExpName = "{EXPNAME}"), (select ChannelId from Channel where ChannelName = "{CHANNELNAME}"), {TimeVAL})""".format(EXPNAME=ExperimentName, CHANNELNAME=ChromatoChannel, TimeVAL = Time)
      CVOrder+=1

    elif(ParseStart == 1 and ChromatoParse == 1 and (parsedData[0] == "binary" and CVOrder == 3 and leadSpace >= 8)):
      #Set Parse Bin Intensity Array to True
      IntensityBin = parsedData[1].split("] ")
      counter = 0
      for Intensity in IntensityBin[1].strip('\n').strip(' ').split(" "): 
        print """INSERT INTO Signal values ((select EID from Exp where ExpName = "{EXPNAME}"), (select ChannelId from Channel where ChannelName = "{CHANNELNAME}"), {SignalVAL})""".format(EXPNAME=ExperimentName, CHANNELNAME=ChromatoChannel, SignalVAL = Intensity)
      CVOrder=0
      ChromatoParse=0
      HashParamListings = hashlib.sha512(ParameterListings).hexdigest()
      SplitParams = ParameterListings.split(",")
      print """INSERT INTO ParameterListing VALUES (DEFAULT, "{ParameterHash}")""".format(ParameterHash = HashParamListings)
      print """INSERT INTO ExParam((select EID from Exp where ExpName = "{EXPNAME}"), (select PID from ParameterListing where ParameterHash = "{ParamHash}"), {AttrID}, "{ParamValue}")""".format(EXPNAME = ExperimentName, ParamHash = HashParamListings, AttrID = ChromatoType, ParamValue = SplitParams[0])
      print """INSERT INTO ExParam((select EID from Exp where ExpName = "{EXPNAME}"), (select PID from ParameterListing where ParameterHash = "{ParamHash}"), {AttrID}, "{ParamValue}")""".format(EXPNAME = ExperimentName, ParamHash = HashParamListings, AttrID = ChromatoIonMode, ParamValue = SplitParams[1])
      print """INSERT INTO RelateExpParameter values ((select EID from Exp where ExpName = "{EXPNAME})", (select PID from ParameterListing where ParameterHash = "{ParamHash}"))""".format(EXPNAME = ExperimentName, ParamHash = HashParamListings)
      ParameterListings = ""
      ChromatoChannel = ""


#Check Duplicates in the Files
DupDict = {}
print [values for key, values in DupDict.items() if len(values) > 1]
#print BinaryArray
