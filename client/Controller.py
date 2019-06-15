import websocket
import threading
import csv
numOfTripsPerDayForYears = {}
idTOPlacesDict = {}
placesTOIdDict = {}
vehiclesIDs = {}
tipsPickedLocationForEachTaxiType = {}
tipsDroppedLocationForEachTaxiType = {}
tripsWithOutPickUPLocation = {}
tripsWithOutDropOffLocation = {}
totalNumberOfRecords = 0
lastID = 0
averageMinPerTrip = {"yellow":0,"green":0,"fhv":0}
numberOfTrip = {"yellow":0,"green":0,"fhv":0}
#################################################################################################################################################################
def readPlacesID():
    global idTOPlacesDict
    global placesTOIdDict
    with open('taxi_zones_simple.csv', mode='r') as infile:
        reader = csv.reader(infile)
        idTOPlacesDict = {rows[0]: rows[1] + "," + rows[2] for rows in reader}
    if "locationid" in idTOPlacesDict:
        del idTOPlacesDict["locationid"]
    placesTOIdDict = {v: k for k, v in idTOPlacesDict.items()}
###############################################################################################################################################################
def prepaire(*taxiTypes):
    readPlacesID()
    global tipsPickedLocationForEachTaxiType
    global tipsDroppedLocationForEachTaxiType
    global tripsWithOutPickUPLocation
    global tripsWithOutDropOffLocation
    for taxiType in taxiTypes:
        tipsPickedLocationForEachTaxiType[taxiType] = {i:0 for i in idTOPlacesDict}
        tipsDroppedLocationForEachTaxiType[taxiType] = {i:0 for i in idTOPlacesDict}
        tripsWithOutPickUPLocation[taxiType] = 0
        tripsWithOutDropOffLocation[taxiType] = 0
###############################################################################################################################################################
def on_message(ws, message):
    listeningThread = threading.Thread(target=processRecored, args=(message,))
    listeningThread.start()
#################################################################################################################################################################
def on_error(ws, error):
    print(error)
#################################################################################################################################################################
def on_close(ws):
    print("### closed ###")
#################################################################################################################################################################
def on_open(ws):
    print("connection opend")
#################################################################################################################################################################
def streamListener():
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("ws://localhost:9000/ws", on_message=on_message, on_error=on_error, on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()
#################################################################################################################################################################
def processRecored(string):
    global numOfTripsPerDayForYears
    global vehiclesIDs
    global totalNumberOfRecords
    global tipsPickedLocationForEachTaxiType
    global tipsDroppedLocationForEachTaxiType
    global tripsWithOutPickUPLocation
    global tripsWithOutDropOffLocation
    totalNumberOfRecords += 1

    taxiType = 0
    pickupLocationId = 0
    string = string[1:-1]
    tempList = str(string).replace('"','').replace("\\","").split(",")
    for item in tempList:
        tempList2 = item.split(" ")
        if len(tempList2) >= 2:
            if "pickupDateTime" in tempList2[0]:
                pickupDate = tempList2[0].split(":")[1].split("-")
                pickUpDateTime = tempList2[1]
                if pickupDate[0] in numOfTripsPerDayForYears:
                    numOfTripsPerDayForYears[pickupDate[0]][pickupDate[1]][pickupDate[2]] += 1

                else:
                    numOfTripsPerDayForYears[pickupDate[0]] = getOneYearDict()
                    numOfTripsPerDayForYears[pickupDate[0]][pickupDate[1]][pickupDate[2]] += 1
            elif "dropOffDatetime" in tempList2[0]:
                dropOffDate = tempList2[0].split(":")[1].split("-")
                dropOffDateTime = tempList2[1]
                tripTime = findTimeInMinutes(pickUpDateTime,dropOffDateTime)
                numberOfTrip[taxiType] += 1
                averageMinPerTrip[taxiType] += ((tripTime)/numberOfTrip[taxiType])
        else:
            tempList2 = item.split(":")
            if "taxiType" in tempList2:
                taxiType = tempList2[1]
            elif "vendorId" in tempList2:
                vendorId = tempList2[1]
                vehiclesIDs [taxiType+","+vendorId] = 1
            elif "pickupLocationId" in tempList2:
                pickupLocationId = tempList2[1]
                if pickupLocationId not in idTOPlacesDict:
                    tripsWithOutPickUPLocation[taxiType] += 1
                else:
                    tipsPickedLocationForEachTaxiType[taxiType][pickupLocationId] += 1
            elif "dropOffLocationId" in tempList2:
                dropOffLocationId = tempList2[1]
                if dropOffLocationId not in idTOPlacesDict:
                    tripsWithOutDropOffLocation[taxiType] += 1
                else:
                    tipsDroppedLocationForEachTaxiType[taxiType][dropOffLocationId] += 1
            elif "type" in tempList2:
                type = tempList2[1]
#################################################################################################################################################################
def findTimeInMinutes(timeOneInString , timeTwoInString):
    timeOneList = str(timeOneInString).split(":")
    timeTwoList = str(timeTwoInString).split(":")

    timeOneHours = int(timeOneList[0])
    timeOneMins = int(timeOneList[1])
    timeOneSec = int(timeOneList[2])

    timeTwoHours = int(timeTwoList[0])
    timeTwoMins = int(timeTwoList[1])
    timeTwoSec = int(timeTwoList[2])

    timeInMinutes = timeTwoHours*60+timeTwoMins+(timeTwoSec/60) -  (timeOneHours*60+timeOneMins+(timeOneSec/60))
    return timeInMinutes
#################################################################################################################################################################
def getOneYearDict():
    return {"01":{"{:02d}".format(i):0 for i in range(1,32)},"02":{"{:02d}".format(i):0 for i in range(1,30)},"03":{"{:02d}".format(i):0 for i in range(1,32)},
        "04":{"{:02d}".format(i):0 for i in range(1,31)},"05":{"{:02d}".format(i):0 for i in range(1,32)},"06":{"{:02d}".format(i):0 for i in range(1,31)},
        "07":{"{:02d}".format(i):0 for i in range(1,32)},"08":{"{:02d}".format(i):0 for i in range(1,32)},"09":{"{:02d}".format(i):0 for i in range(1,31)},
        "10":{"{:02d}".format(i):0 for i in range(1,32)},"11":{"{:02d}".format(i):0 for i in range(1,31)},"12":{"{:02d}".format(i):0 for i in range(1,32)}}
#################################################################################################################################################################
def getNumberOfTripsperDay():
    global numOfTripsPerDayForYears
    temp = numOfTripsPerDayForYears
    tripsList = []
    for monthKeys in temp["2017"]:
        tripsList.extend(list(temp["2017"][monthKeys].values()))
    return range(max(tripsList)),tripsList

def calculateAverageTripsPerDayForMonth(year,monthIndex):
    global numOfTripsPerDayForYears
    temp = numOfTripsPerDayForYears
    sum = 0
    for value in temp[year][monthIndex].values():
        sum += value
    return sum/len(temp[year][monthIndex])


def writeInFile():
    global placesTOIdDict
    f=open("result data.txt","w")
    f.write(str(totalNumberOfRecords)+",")
    f.write(str(totalNumberOfRecords)+",")
    f.write(str(calculateAverageTripsPerDayForMonth("2017", "11"))+",")
    f.write(str(len(vehiclesIDs))+",")
    f.write(str(tipsPickedLocationForEachTaxiType["yellow"][placesTOIdDict["Woodside,Queens"]])+",")
    f.write(str(tipsPickedLocationForEachTaxiType["green"][placesTOIdDict["Woodside,Queens"]])+",")
    f.write(str(tipsPickedLocationForEachTaxiType["fhv"][placesTOIdDict["Woodside,Queens"]]))
    f.close()

def run():
    prepaire("yellow", "green", "fhv")
    listeningThread = threading.Thread(target=streamListener, args=())
    listeningThread.start()

