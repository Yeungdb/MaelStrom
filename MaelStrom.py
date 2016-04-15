#!/usr/bin/python

import hashlib
from optparse import OptionParser
from glob import glob


def MaelStromParser(filename):
    file = open(filename, 'r')
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
    SpectrumParse = 0
    #---------------------------

    #---------------------------
    #Defining Global Vars
    FileId = 0
    ChromatoId = 0
    ChromatoType = 0 #TIC, SRM, MSMS for SplitParam[0]
    ChromatoIonMode = 1 #Positive/Negativ for SplitParam[1]
    MZLowerLimit = 3 #Binary Index in Array for Binary Data for Time 
    MZUpperLimit = 4 #Binary Indexin Array for Binary Data for Intensity

    binId = 0
    #---------------------------

    #---------------------------
    #Loop Vars

    ExperimentName = ""
    ParameterListings = ""
    ChromatoChannel = ""
    ScanStartTime = "" #For TIC Full Scan Spectras

    #---------------------------


    numLines = 0
    for linesOpt in file:
        numLines += 1

    ParameterListing = ""

    file.seek(0) #Refresh the File Cursor for reiteration

    BinaryArray = []
    for x in xrange(2):
        BinaryArray.append([])

    for lines in file:
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

        elif(ParseStart == 1 and (parsedData[0] == "spectrum" and leadSpace == 6)): #Based on the File Hierarchy, 2->Run, 4->Id
          SpectrumParse = 1
        
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
          print """INSERT INTO ExParam values ((select EID from Exp where ExpName = "{EXPNAME}"), (select PID from ParameterListing where ParameterHash = "{ParamHash}"), {AttrID}, "{ParamValue}")""".format(EXPNAME = ExperimentName, ParamHash = HashParamListings, AttrID = ChromatoType, ParamValue = SplitParams[0])
          print """INSERT INTO ExParam values ((select EID from Exp where ExpName = "{EXPNAME}"), (select PID from ParameterListing where ParameterHash = "{ParamHash}"), {AttrID}, "{ParamValue}")""".format(EXPNAME = ExperimentName, ParamHash = HashParamListings, AttrID = ChromatoIonMode, ParamValue = SplitParams[1])
          print """INSERT INTO RelateExpParameter values ((select EID from Exp where ExpName = "{EXPNAME})", (select PID from ParameterListing where ParameterHash = "{ParamHash}"))""".format(EXPNAME = ExperimentName, ParamHash = HashParamListings)
          ParameterListings = ""
          ChromatoChannel = ""

        elif(ParseStart == 1 and SpectrumParse == 1 and parsedData[0] == "cvParam" and ("positive" in parsedData[1]) or ("negative" in parsedData[1])):
          TICScanType = parsedData[1].lstrip(' ').strip('\n')
          FileNameHash = hashlib.sha512(ExperimentName).hexdigest() #TIC FullScan Spectrums will use FileNameHash rather than Chromatogram Specific Hash since there is only one channel of signals
          print """INSERT INTO ParameterListing VALUES (DEFAULT, "{ParameterHash}")""".format(ParameterHash = FileNameHash)
          print """INSERT INTO ExParam values ((select EID from Exp where ExpName = "{EXPNAME}"), (select PID from ParameterListing where ParameterHash = "{ParamHash}"), {AttrID}, "{ParamValue}")""".format(EXPNAME = ExperimentName, ParamHash = FileNameHash, AttrID = ChromatoType, ParamValue = TICScanType) #ChromatoType is for ScanMode
          print """INSERT INTO RelateExpParameter values ((select EID from Exp where ExpName = "{EXPNAME})", (select PID from ParameterListing where ParameterHash = "{ParamHash}"))""".format(EXPNAME = ExperimentName, ParamHash = FileNameHash)
          print """INSER INTO Channel values (DEFAULT, {ChannelName})""".format(ChannelName = ExperimentName)

        elif(ParseStart == 1 and SpectrumParse == 1 and parsedData[0] == "cvParam" and "scan start time" in parsedData[1]):
          ScanStartTime = parsedData[1].lstrip(' ').strip('\n').split(",")[1]
     
        elif(ParseStart == 1 and SpectrumParse == 1 and parsedData[0] == "cvParam" and "scan window" in parsedData[1]):
          FileNameHash = hashlib.sha512(ExperimentName).hexdigest() #TIC FullScan Spectrums will use FileNameHash rather than Chromatogram Specific Hash since there is only one channel of signals
          if("lower limit" in parsedData[1]):
             lowerLimit = parsedData[1].lstrip(' ').strip('\n').split(', ')[1]
             print """INSERT INTO ExParam values ((select EID from Exp where ExpName = "{EXPNAME}"), (select PID from ParameterListing where ParameterHash = "{ParamHash}"), {AttrID}, "{ParamValue}")""".format(EXPNAME = ExperimentName, ParamHash = FileNameHash, AttrID = MZLowerLimit, ParamValue = lowerLimit) #ChromatoType is for ScanMode

          elif("upper limit" in parsedData[1]):
             upperLimit = parsedData[1].lstrip(' ').strip('\n').split(', ')[1]
             print """INSERT INTO ExParam values ((select EID from Exp where ExpName = "{EXPNAME}"), (select PID from ParameterListing where ParameterHash = "{ParamHash}"), {AttrID}, "{ParamValue}")""".format(EXPNAME = ExperimentName, ParamHash = FileNameHash, AttrID = MZUpperLimit, ParamValue = upperLimit) #ChromatoType is for ScanMode

        elif(ParseStart == 1 and SpectrumParse == 1 and parsedData[0]=="binary" and binId == 0):
          MZBin = parsedData[1].split("] ")
          for MZ in MZBin[1].strip('\n').strip(" ").split(" "):
            print """INSERT INTO TICMZ values ((select EID from Exp where ExpName = "{EXPNAME}"), (select ChannelId from Channel where ChannelName = "{ChannelName}"), {TimeStamp}, {MZ})""".format(EXPNAME = ExperimentName, ChannelName = ExperimentName, TimeStamp = ScanStartTime, MZ = MZ) 
          binId += 1

        elif(ParseStart == 1 and SpectrumParse == 1 and parsedData[0]=="binary" and binId == 1):
          IntensityBin = parsedData[1].split("] ")
          for Intensity in IntensityBin[1].strip('\n').strip(" ").split(" "):
            print """INSERT INTO TICINT values ((select EID from Exp where ExpName = "{EXPNAME}"), (select ChannelId from Channel where ChannelName = "{ChannelName}"), {TimeStamp}, {Intensity})""".format(EXPNAME = ExperimentName, ChannelName = ExperimentName, TimeStamp = ScanStartTime, Intensity = Intensity) 
          binId = 0


parser = OptionParser()
parser.add_option('-f', "--filename", help="Filename", action="store")

options, args = parser.parse_args()

fileList = glob("{FILENAME}".format(FILENAME=options.filename))
print fileList
for file in fileList:
    MaelStromParser(file)
